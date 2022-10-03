import asyncio
import contextlib
import logging
import os
import sys
import typing as ty
import pandas as pd
import win32com.client as win32
import datetime
import numpy as np 
import time 

from act import connection
from act import dex
from act import session
from act.util import logutil
from act.util import util

class Container:
    
    def __init__(self,data,email):
        self.data = data
        self.email = email

logger = logging.getLogger(__name__)
script_name = os.path.basename(sys.argv[0])

SAMPLE_USAGE = {
    r'Get BID,ASK snapshot on XCME.ES.F':
        [
            f"{script_name}  --user Shared --password ' --fields BID,ASK --scope_keys XCME.ES.F --ip 192.168.45.117 --snapshot ",
        ],
    r'Get XBIT.BTC.O PEs into a csv file (to update and write back using dex_table_update.py)':
        [
            f"{script_name}  --user Shared --password ' --fields PE1,PE2,PE3 --scope_keys XBIT.BTC.O --ip 192.168.45.117 --port 4724 --snapshot --output_csv_path C:\dev\BTC_PEs.csv",
        ]
}
    
def send_email():
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        #recipients = ['igor', 'pete', 'keng', 'lak', 'chris','Brian', 'giri','marc','fiona', 'dan']
        recipients = ['marc']
        recipient_emails = ''
        for person in recipients: 
            recipient_emails += person + '@elequincapital.com;'
        mail.To = recipient_emails[:-1]
        mail.Subject = 'IMPORTANT: Stopped Market Making'
        mail.Body = "We have stopped making markets in some ticker. Please check accordingly"
        print("Email was sent.")
        mail.Send()
        return

async def run(
        start,
        ip: str,
        port: int,
        user: str,
        password: str,
        scope_keys: ty.List[str],
        fields: ty.List[str],
        frequency: int,
        is_snapshot: bool,
        no_triggers: ty.Optional[ty.List[str]] = None,
        contexts: ty.Optional[ty.List[str]] = None,
        output_csv_path: ty.Optional[str] = None,
):
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handler=util.handle_asyncio_exceptions)
    
    container = start
    
    act_connection = connection.ActConnection(ip=ip, port=port, loop=loop)
    try:
        await act_connection.connect()
        if not act_connection.is_connected():
            return

        act_session = session.ActSession(act_connection=act_connection, user=user, password=password, appname=script_name)
        on_logon: session.LogonResponse = await act_session.logon()
        if on_logon.Success:
            logger.info(f'Successfully logged in')
        else:
            logger.info(f'Failed to log in. msg:"{on_logon.ErrorMsg}"')
            return

        def on_query_state_change(dq: dex.DexQuery, new_status: dex.DexQueryState, err_msg: str, old_status: dex.DexQueryState):
            if err_msg is None or len(err_msg) == 0:
                logger.info(f'Query state changed: {new_status} (from {old_status})')
            else:
                logger.info(f'Query state changed: {new_status} (from {old_status}), msg:{err_msg}')
                act_connection.disconnect()

        def on_columns_received(dq: dex.DexQuery, columns: ty.List[dex.DexColumn]):
            print(columns)
            # print(dir(columns[-1]))
            print([col.is_vector for col in columns])
            logger.info(f'Columns received. NumColumns:{len(columns)}')

        def on_update(dq: dex.DexQuery, update_count: int, num_rows: int, new_rows: ty.List[dex.DexRow], new_updated_rows: ty.List[dex.DexRow]):
            logger.info(f'Query update. UpdateCount:{update_count}, NumRows:{num_rows}, NumNewRows:{len(new_rows)}, NumNewUpdatedRows: {len(new_updated_rows)}')

            def get_value_from_variant(variant):
                str_split = str(variant).replace('\n','').replace(' ', '').split(':')
                type = str_split[0]
                raw_value = str_split[1]
                if type == 'varDouble':
                    return float(raw_value)
                return raw_value.replace('"', '')
        
            #tickers =['XUSZ.APE']
            
            for row in new_updated_rows:
                cells = row.cells
                row_dict = dict()
                for cell in cells:
                    if cell.value is not None:
                        row_dict[cell.column.name] = cell.value_str()
                    elif cell.vector is not None:
                        row_dict[cell.column.name] = [get_value_from_variant(x) for x in cell.vector]

                container.data.append(row_dict)
            
            #eventually just turn this all into a dictionary
            update_result_dict = []
            for i in container.data:
                    update_result_dict.append(i)
                       
            df = pd.DataFrame.from_dict(update_result_dict)
            cols = df.columns
            df[cols] = df[cols].apply(pd.to_numeric, errors='ignore')
            
    
            df.rename(columns = {"ORDER.INSTRUMENT": "Symbol"}, inplace = True)
            df = df.query(f"Symbol in {scope_keys}")
            df.set_index('ORDER.ORDERKEY', inplace = True)
            df = df.iloc[::-1]
            df = df[~df.index.duplicated()]
            df = df[(df["ORDER.STATUS"] == "Active") | (df["ORDER.STATUS"] == 'Updating')]
            
            df['Spread'] = (df['UNDER.BID'] - df["ORDER.PRICE"])/df['UNDER.BID']
            mask = df['ORDER.SIDE'] == "Sell"
            df.loc[mask,'Spread'] = (df["ORDER.PRICE"] - df['UNDER.ASK'])/df['UNDER.ASK']
            
            spread_array = df['Spread'].to_numpy()
            spread_array = spread_array >= .06
            
            df_count = df.groupby('Symbol').size().reset_index(name='Size')
            count_array = df_count['Size'].to_numpy()
            count_array = count_array < 2

            print(df_count)
            
            #sends out email if: we are no longer quoting, only quoting one side, or if the spread is too wide 
            if ((count_array.sum() != 0) or (set(df_count['Symbol'].to_numpy()) != set(scope_keys)) or (spread_array.sum() != 0)): #and container.email:
                send_email()
                print("ERORR ERROR")
                time.sleep(45)
                container.email = False
            
            if output_csv_path is not None:
                csv = dq.as_csv()
                # https://stackoverflow.com/a/3191811
                with open(file=output_csv_path, mode='w', newline='', encoding='utf-8') as out_csv_file:
                    out_csv_file.write(csv)
                    out_csv_file.flush()

            if is_snapshot:
                dq.stop()
                act_connection.disconnect()

        query_data = dex.DexQueryData(scope_keys=scope_keys, fields=fields, frequency=frequency, is_snapshot=is_snapshot, no_triggers=no_triggers, contexts=contexts)
        dex_query = dex.DexQuery(act_session=act_session, query_data=query_data)
        dex_query.add_handlers(state_change_handler=on_query_state_change, columns_received_handler=on_columns_received, update_handler=on_update)
        dex_query.start()

        await act_connection.wait_on_disconnect()
    finally:
        if act_connection is not None:
            act_connection.disconnect()
        pending = asyncio.all_tasks()
        for task in pending:
            if task.cancelled():
                continue
            task.cancel()
            with contextlib.suppress(asyncio.exceptions.CancelledError):
                await task

def check_active(path):
    activity_df = pd.read_csv(path)
    activity_df = activity_df[activity_df['ORDER.STATUS'] == "Active"]
    
    activity_df.to_csv(path)
    print(activity_df)
    
def main():
    parser = util.get_arg_parser(desc="Run a DEX query", examples=SAMPLE_USAGE)
    util.add_act_connection_args(parser=parser)
    #parser.add_argument('-s', '--scope_keys', help='The DEX scope keys', required = True)
    #parser.add_argument('-f', '--fields', help='The DEX fields', required = True)
    #parser.add_argument('-ntf', '--non_triggering_fields', help='The non-triggering fields')
    parser.add_argument('-c', '--context', help='Context for the query')
    parser.add_argument('-sn', '--snapshot', help='Is snapshot query', action='store_true')
    parser.add_argument('-fr', '--frequency', help='Frequency for non-snapshot queries', default=1000, type=int)
    parser.add_argument('-out_csv', '--output_csv_path', help='Path to csv file to create or overwrite with dex query output')
    parser.add_argument('-ll', '--loglevel', help='Level at which to log (DEBUG, INFO, WARNING, ERROR, CRITICAL)', required=False, default='INFO')
    parser.set_defaults(snapshot=False)
    args = parser.parse_args()
    logutil.configure_simple_console_logging(loglevel=args.loglevel, timed=True)
    
    #pulls mm names from list 
    today = datetime.datetime.today().strftime('%Y%m%d')
    excel_file = os.path.expanduser(f"~//OneDrive - QEC Capital//Documents//Operations//CBOE_MM_names//CBOE_mm_names_{today}.txt")
    tickers = pd.read_csv(excel_file, header = None)
    tickers_list = tickers[0].tolist()
    tickers_list= list(map(lambda x: "XUSZ." + x, tickers_list))
    
    #test ticker
    #tickers_list = ['XUSZ.']
    
    container = Container( data = [], email = True)
    
    scope_keys = tickers_list
    fields = ['ORDER.ORDERKEY','ORDER.INSTRUMENT','ORDER.SIDE','ORDER.PRICE','ORDER.QUANTITY','ORDER.ACTIVEQUANTITY','ORDER.STATUS', 'UNDER.BID', 'UNDER.ASK']
    non_triggering_fields = ['ORDER.PRICE','ORDER.QUANTITY','ORDER.ACTIVEQUANTITY']
    output_csv_path = None
        
    context = None
    if args.context is not None:
        context = args.context.split(',')
        
    try:
        asyncio.run(run(
            start = container,
            ip=args.ip, port=args.port,
            user=args.user, password=args.password,
            scope_keys=scope_keys, fields=fields,
            frequency=args.frequency, is_snapshot=args.snapshot,
            no_triggers=non_triggering_fields, contexts=context,
            output_csv_path= output_csv_path
        ))
    except KeyboardInterrupt:
        logger.info(f'Exiting on Ctrl-C')
    
    #check_active(path)

if __name__ == '__main__':
    try:
        main()
    except Exception:
        logger.exception('Caught exception in main()')
