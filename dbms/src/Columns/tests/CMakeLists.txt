set(SRCS)

add_executable (column_unique column_unique.cpp ${SRCS})
target_link_libraries (column_unique PRIVATE dbms gtest_main)
