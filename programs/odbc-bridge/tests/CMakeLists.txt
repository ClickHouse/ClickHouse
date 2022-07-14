clickhouse_add_executable (validate-odbc-connection-string validate-odbc-connection-string.cpp ../validateODBCConnectionString.cpp)
target_link_libraries (validate-odbc-connection-string PRIVATE clickhouse_common_io)
