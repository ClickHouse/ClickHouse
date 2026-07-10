clickhouse_add_executable(benchmark_statistics benchmark_statistics.cpp)
target_link_libraries(benchmark_statistics PRIVATE
    ch_contrib::gbenchmark_all
    dbms)
