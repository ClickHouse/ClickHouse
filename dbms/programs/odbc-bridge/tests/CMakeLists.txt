add_executable (validate-odbc-connection-string validate-odbc-connection-string.cpp)
target_link_libraries (validate-odbc-connection-string PRIVATE clickhouse-odbc-bridge-lib clickhouse_common_io)
