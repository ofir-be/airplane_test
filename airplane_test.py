import numpy as np
import pandas as pd
import snowflake.connector
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from snowflake.connector.pandas_tools import pd_writer
import os
#pip install "snowflake-connector-python[pandas]"



def main(params):
    
    snowflake_account_name = os.getenv('SNOWFLAKE_ACCOUNT_NAME')
    snowflake_user = os.getenv('SNOWFLAKE_USER') 
    snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')  
    role_name = 'BI_DEV_ROLE'

    def create_db_engine(db_name, schema_name):


        engine = URL(
            account = snowflake_account_name,
            user = snowflake_user,
            password = snowflake_password,
            role = role_name,
            database = db_name,
            schema = schema_name
            #warehouse="WH1",
            #role="DEV"
        )
        return engine

    def create_table(out_df, table_name, if_exists, db_name, schema_name, idx=False):
        url = create_db_engine(db_name = db_name, schema_name = schema_name)
        engine = create_engine(url)

        try:
            out_df.to_sql(
                table_name, engine, if_exists = if_exists, index = idx, method = pd_writer
            )

        except ConnectionError:
            print("Unable to connect to database!")

        finally:
            engine.dispose()

        return True



    l1 = ['Active Fence_2', 260]
    df2 = pd.DataFrame(columns = ['COMPANY_NAME', 'CURRENT_COVERAGE'])
    new_df = pd.DataFrame([l1], columns = ['COMPANY_NAME', 'CURRENT_COVERAGE'])
    df2 = pd.concat([df2,new_df], axis=0, ignore_index=True)

    #create table in snowflake
    create_table(out_df = df2, 
                table_name = 'airplane_test', 
                if_exists = "replace",
                db_name = "DEMO_DB",
                schema_name = "PUBLIC")
    
    return 'success'
