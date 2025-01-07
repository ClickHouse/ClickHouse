clickhouse_add_executable (validate-odbc-connection-string validate-odbc-connection-string.cpp ../validateODBCConnectionString.cpp)
target_link_libraries (validate-odbc-connection-string PRIVATE dbms clickhouse_functions clickhouse_common_io clickhouse_common_config)
