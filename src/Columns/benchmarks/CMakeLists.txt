clickhouse_add_executable(column_insert_many_from benchmark_column_insert_many_from.cpp)
target_link_libraries (column_insert_many_from PRIVATE
    ch_contrib::gbenchmark_all
    dbms
    clickhouse_functions)
