clickhouse_add_microbenchmark(benchmark_executors
    SOURCES benchmark_executors.cpp Shapes.cpp
    LIBRARIES dbms clickhouse_functions)
