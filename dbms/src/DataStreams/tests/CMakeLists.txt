set(SRCS )

add_executable (expression_stream expression_stream.cpp ${SRCS})
target_link_libraries (expression_stream dbms clickhouse_storages_system)

add_executable (filter_stream filter_stream.cpp ${SRCS})
target_link_libraries (filter_stream dbms clickhouse_storages_system)

add_executable (union_stream2 union_stream2.cpp ${SRCS})
target_link_libraries (union_stream2 dbms)

add_executable (collapsing_sorted_stream collapsing_sorted_stream.cpp ${SRCS})
target_link_libraries (collapsing_sorted_stream dbms)
