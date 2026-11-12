clickhouse_add_microbenchmark(benchmark_hashed_join_key
    SOURCES benchmark_hashed_join_key.cpp
    LIBRARIES dbms)

clickhouse_add_microbenchmark(benchmark_packed_string_join_key
    SOURCES benchmark_packed_string_join_key.cpp
    LIBRARIES dbms)
