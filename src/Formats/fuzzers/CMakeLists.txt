clickhouse_add_executable(format_fuzzer format_fuzzer.cpp ${SRCS})
target_link_libraries(format_fuzzer PRIVATE clickhouse_aggregate_functions dbms)

clickhouse_add_executable(native_reader_fuzzer native_reader_fuzzer.cpp native_reader_custom_mutator.cpp ${SRCS})
target_link_libraries(native_reader_fuzzer PRIVATE clickhouse_aggregate_functions dbms)

if (USE_PARQUET OR USE_AVRO)
    clickhouse_add_executable(parquet_variant_fuzzer parquet_variant_fuzzer.cpp ${SRCS})
    target_link_libraries(parquet_variant_fuzzer PRIVATE dbms)
endif()
