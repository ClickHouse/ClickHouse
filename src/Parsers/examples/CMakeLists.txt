set(SRCS)

clickhouse_add_executable(lexer lexer.cpp ${SRCS})
target_link_libraries(lexer PRIVATE clickhouse_parsers clickhouse_common_config)

clickhouse_add_executable(select_parser select_parser.cpp ${SRCS} "../../Server/ServerType.cpp")
target_link_libraries(select_parser PRIVATE dbms)

clickhouse_add_executable(create_parser create_parser.cpp ${SRCS} "../../Server/ServerType.cpp")
target_link_libraries(create_parser PRIVATE dbms)

clickhouse_add_executable(parser_memory_profiler parser_memory_profiler.cpp ${SRCS} "../../Server/ServerType.cpp")
target_link_libraries(parser_memory_profiler PRIVATE dbms)
if (OS_LINUX)
    # Build as non-PIE so jemalloc heap profile addresses are stable across processes,
    # enabling cross-process batch symbolization with --symbolize-batch.
    target_link_options(parser_memory_profiler PRIVATE -no-pie -Wl,-no-pie)
endif()
