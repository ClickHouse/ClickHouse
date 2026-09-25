clickhouse_add_executable(sparql_translation_benchmark benchmark_sparql_translation.cpp)
target_link_libraries(sparql_translation_benchmark PRIVATE
    ch_contrib::gbenchmark_all
    clickhouse_parsers)
