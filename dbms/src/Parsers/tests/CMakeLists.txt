include_directories (${CMAKE_CURRENT_BINARY_DIR})

set(SRCS )

add_executable (lexer lexer.cpp ${SRCS})
target_link_libraries (lexer dbms clickhouse_parsers)

add_executable (select_parser select_parser.cpp ${SRCS})
target_link_libraries (select_parser dbms clickhouse_parsers)

add_executable (create_parser create_parser.cpp ${SRCS})
target_link_libraries (create_parser dbms clickhouse_parsers)

