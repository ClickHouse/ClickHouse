clickhouse_add_executable(format_fuzzer format_fuzzer.cpp ${SRCS})
target_link_libraries(format_fuzzer PRIVATE dbms clickhouse_aggregate_functions ${LIB_FUZZING_ENGINE})
