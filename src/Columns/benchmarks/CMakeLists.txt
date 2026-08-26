clickhouse_add_microbenchmark(column_insert_many_from
    SOURCES benchmark_column_insert_many_from.cpp
    LIBRARIES dbms)

clickhouse_add_microbenchmark(benchmark_compute_hash_into
    SOURCES benchmark_compute_hash_into.cpp
    LIBRARIES dbms)
