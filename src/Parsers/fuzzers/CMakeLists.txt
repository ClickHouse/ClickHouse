add_executable(lexer_fuzzer lexer_fuzzer.cpp ${SRCS})
target_link_libraries(lexer_fuzzer PRIVATE clickhouse_parsers ${LIB_FUZZING_ENGINE})

add_executable(select_parser_fuzzer select_parser_fuzzer.cpp ${SRCS})
target_link_libraries(select_parser_fuzzer PRIVATE clickhouse_parsers ${LIB_FUZZING_ENGINE})

add_executable(create_parser_fuzzer create_parser_fuzzer.cpp ${SRCS})
target_link_libraries(create_parser_fuzzer PRIVATE clickhouse_parsers ${LIB_FUZZING_ENGINE})

add_subdirectory(codegen_fuzzer)

