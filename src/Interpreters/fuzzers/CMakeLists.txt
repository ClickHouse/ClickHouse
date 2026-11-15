clickhouse_add_executable(execute_query_fuzzer execute_query_fuzzer.cpp)
target_link_libraries(execute_query_fuzzer PRIVATE
    dbms
    clickhouse_table_functions
    clickhouse_aggregate_functions
    clickhouse_functions
    clickhouse_dictionaries
    clickhouse_dictionaries_embedded)
