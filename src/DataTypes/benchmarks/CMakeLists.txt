clickhouse_add_executable(benchmark_enum_storage benchmark_enum_storage.cpp)
target_link_libraries(benchmark_enum_storage PRIVATE
    ch_contrib::gbenchmark_all
    dbms)
