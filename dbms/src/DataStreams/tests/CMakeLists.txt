include_directories (${CMAKE_CURRENT_BINARY_DIR})

set(SRCS )

add_executable (tab_separated_streams tab_separated_streams.cpp ${SRCS})
target_link_libraries (tab_separated_streams dbms)

add_executable (block_row_transforms block_row_transforms.cpp ${SRCS})
target_link_libraries (block_row_transforms dbms)

add_executable (expression_stream expression_stream.cpp ${SRCS})
target_link_libraries (expression_stream dbms clickhouse_storages_system)

add_executable (native_streams native_streams.cpp ${SRCS})
target_link_libraries (native_streams dbms)

add_executable (filter_stream filter_stream.cpp ${SRCS})
target_link_libraries (filter_stream dbms clickhouse_storages_system)

add_executable (filter_stream_hitlog filter_stream_hitlog.cpp ${SRCS})
target_link_libraries (filter_stream_hitlog dbms)

add_executable (sorting_stream sorting_stream.cpp ${SRCS})
target_link_libraries (sorting_stream dbms)

add_executable (union_stream2 union_stream2.cpp ${SRCS})
target_link_libraries (union_stream2 dbms)

add_executable (collapsing_sorted_stream collapsing_sorted_stream.cpp ${SRCS})
target_link_libraries (collapsing_sorted_stream dbms)
