set(SRCS)

add_executable (expression_stream expression_stream.cpp ${SRCS})
target_link_libraries (expression_stream PRIVATE dbms clickhouse_storages_system clickhouse_parsers)

add_executable (filter_stream filter_stream.cpp ${SRCS})
target_link_libraries (filter_stream PRIVATE dbms clickhouse_storages_system clickhouse_parsers clickhouse_common_io)

add_executable (union_stream2 union_stream2.cpp ${SRCS})
target_link_libraries (union_stream2 PRIVATE dbms)

add_executable (finish_sorting_stream finish_sorting_stream.cpp ${SRCS})
target_link_libraries (finish_sorting_stream PRIVATE dbms)
