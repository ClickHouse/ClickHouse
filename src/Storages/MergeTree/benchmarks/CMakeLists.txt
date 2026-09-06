clickhouse_add_executable(benchmark_projection_index_bitmap projection_index_bitmap.cpp)
target_link_libraries (benchmark_projection_index_bitmap PRIVATE
    ch_contrib::gbenchmark_all
    dbms)
