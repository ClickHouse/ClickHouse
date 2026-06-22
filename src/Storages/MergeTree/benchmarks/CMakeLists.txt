clickhouse_add_executable(benchmark_posting_list_codec benchmark_posting_list_codec.cpp)
target_link_libraries (benchmark_posting_list_codec PRIVATE
    ch_contrib::gbenchmark_all
    dbms)
