add_executable (validate-odbc-connection-string validate-odbc-connection-string.cpp)
clickhouse_target_link_split_lib(validate-odbc-connection-string odbc-bridge)
target_link_libraries (validate-odbc-connection-string PRIVATE clickhouse_common_io)
