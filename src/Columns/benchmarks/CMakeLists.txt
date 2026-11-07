clickhouse_add_microbenchmark(column_insert_many_from
    SOURCES benchmark_column_insert_many_from.cpp
    LIBRARIES dbms)

clickhouse_add_microbenchmark(benchmark_compute_hash_into
    SOURCES benchmark_compute_hash_into.cpp
    LIBRARIES dbms)

clickhouse_add_microbenchmark(benchmark_columns_scatter
    SOURCES benchmark_columns_scatter.cpp
    LIBRARIES dbms)
# The arms this tree can compile: 0 for `master`'s `scatter`, 1 with `SmallSelector` and `scatterBlock`,
# 2 with the batched entry of `ColumnsScatter`.
target_compile_definitions(benchmark_columns_scatter_objects PRIVATE SCATTER_BENCH_LEVEL=2)
