set(SRCS )

add_executable (tab_separated_streams tab_separated_streams.cpp ${SRCS})
target_link_libraries (tab_separated_streams dbms)

add_executable (block_row_transforms block_row_transforms.cpp ${SRCS})
target_link_libraries (block_row_transforms dbms)
