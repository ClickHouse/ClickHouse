add_executable(lexer_fuzzer lexer_fuzzer.cpp ${SRCS})
target_link_libraries(lexer_fuzzer PRIVATE clickhouse_parsers ${LIB_FUZZING_ENGINE})

add_executable(select_parser_fuzzer select_parser_fuzzer.cpp ${SRCS})
target_link_libraries(select_parser_fuzzer PRIVATE clickhouse_parsers ${LIB_FUZZING_ENGINE})

add_executable(create_parser_fuzzer create_parser_fuzzer.cpp ${SRCS})
target_link_libraries(create_parser_fuzzer PRIVATE clickhouse_parsers ${LIB_FUZZING_ENGINE})

string(REPLACE " -Werror" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
add_subdirectory(codegen_fuzzer)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")

