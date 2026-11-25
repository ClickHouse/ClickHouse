clickhouse_add_microbenchmark(benchmark_statistics
    SOURCES benchmark_statistics.cpp
    LIBRARIES dbms)

clickhouse_add_microbenchmark(benchmark_read_ranges_registry
    SOURCES benchmark_read_ranges_registry.cpp
    LIBRARIES dbms)
