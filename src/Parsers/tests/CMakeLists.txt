set(SRCS)

add_executable(lexer lexer.cpp ${SRCS})
target_link_libraries(lexer PRIVATE clickhouse_parsers)

add_executable(select_parser select_parser.cpp ${SRCS})
target_link_libraries(select_parser PRIVATE clickhouse_parsers)

add_executable(create_parser create_parser.cpp ${SRCS})
target_link_libraries(create_parser PRIVATE clickhouse_parsers)
