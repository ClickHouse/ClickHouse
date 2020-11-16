set(SRCS)

add_executable (finish_sorting_stream finish_sorting_stream.cpp ${SRCS})
target_link_libraries (finish_sorting_stream PRIVATE clickhouse_aggregate_functions dbms)
