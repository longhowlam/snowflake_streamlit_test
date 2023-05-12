import streamlit as st
from snowflake.snowpark import Session

#### Current Environment Details
def current_snowflake_env():
    snowflake_environment = session.sql('select current_user(), current_role(), current_database(), current_schema(), current_version(), current_warehouse()').collect()
    print('User                     : {}'.format(snowflake_environment[0][0]))
    print('Role                     : {}'.format(snowflake_environment[0][1]))
    print('Database                 : {}'.format(snowflake_environment[0][2]))
    print('Schema                   : {}'.format(snowflake_environment[0][3]))
    print('Warehouse                : {}'.format(snowflake_environment[0][5]))
    print('Snowflake version        : {}'.format(snowflake_environment[0][4]))


#### Snowflake Connection Parameters
connection_parameters = {
    "account":  st.secrets["snowflake_account"], 
    "user":  st.secrets["snowflake_user"],
    "password":  st.secrets["snowflake_password"],
    "warehouse": "COMPUTE_WH",
    "role": "ACCOUNTADMIN",
    "database": "SNOWFLAKE_SAMPLE_DATA",
    "schema": "TPCH_SF10"
}

#### Set up a connection with Snowflake
session = Session.builder.configs(connection_parameters).create()
current_snowflake_env()

wh =session.get_current_database()
st.write ("Current Warehouse: ")
st.write(wh)
st.write(session)

df = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER").limit(5).to_pandas()

st.write("Snowflake Data")
st.write(df)
