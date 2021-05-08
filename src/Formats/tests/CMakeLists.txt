set(SRCS )

add_executable (tab_separated_streams tab_separated_streams.cpp ${SRCS})
target_link_libraries (tab_separated_streams PRIVATE clickhouse_aggregate_functions dbms)
