clickhouse_add_executable(runtime_filter runtime_filter.cpp)
target_link_libraries(runtime_filter PRIVATE
    ch_contrib::gbenchmark_all
    clickhouse_functions
    dbms)
