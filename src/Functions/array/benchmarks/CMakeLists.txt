clickhouse_add_executable(benchmark_array_random_projection benchmark_array_random_projection.cpp)
target_link_libraries(benchmark_array_random_projection PRIVATE
    ch_contrib::gbenchmark_all
    dbms)
