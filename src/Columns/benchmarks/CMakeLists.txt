clickhouse_add_executable(column_insert_many_from benchmark_column_insert_many_from.cpp)
target_link_libraries (column_insert_many_from PRIVATE
    ch_contrib::gbenchmark_all
    dbms)

clickhouse_add_executable(benchmark_compute_hash_into benchmark_compute_hash_into.cpp)
target_link_libraries (benchmark_compute_hash_into PRIVATE
    ch_contrib::gbenchmark_all
    dbms)

clickhouse_add_executable(column_low_cardinality_key_holder benchmark_low_cardinality_key_holder.cpp)
target_link_libraries(column_low_cardinality_key_holder PRIVATE
    ch_contrib::gbenchmark_all
    dbms)
