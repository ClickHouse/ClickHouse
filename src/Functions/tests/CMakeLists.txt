add_executable (number_traits number_traits.cpp)
add_executable (abtesting abtesting.cpp)
target_link_libraries (number_traits PRIVATE dbms)
target_link_libraries (abtesting PRIVATE clickhouse_functions)
