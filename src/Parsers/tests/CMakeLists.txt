set(SRCS)

add_executable(lexer lexer.cpp ${SRCS})
target_link_libraries(lexer PRIVATE clickhouse_parsers)

add_executable(select_parser select_parser.cpp ${SRCS})
target_link_libraries(select_parser PRIVATE clickhouse_parsers)

add_executable(create_parser create_parser.cpp ${SRCS})
target_link_libraries(create_parser PRIVATE clickhouse_parsers)

if (ENABLE_FUZZING)
    add_executable(lexer_fuzzer lexer_fuzzer.cpp ${SRCS})
    target_link_libraries(lexer_fuzzer PRIVATE clickhouse_parsers ${LIB_FUZZING_ENGINE})

    add_executable(select_parser_fuzzer select_parser_fuzzer.cpp ${SRCS})
    target_link_libraries(select_parser_fuzzer PRIVATE clickhouse_parsers ${LIB_FUZZING_ENGINE})

    add_executable(create_parser_fuzzer create_parser_fuzzer.cpp ${SRCS})
    target_link_libraries(create_parser_fuzzer PRIVATE clickhouse_parsers ${LIB_FUZZING_ENGINE})
endif ()
