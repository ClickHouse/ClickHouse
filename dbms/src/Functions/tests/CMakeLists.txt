include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (number_traits number_traits.cpp)
target_link_libraries (number_traits dbms)

add_executable (functions_arithmetic functions_arithmetic.cpp)
target_link_libraries (functions_arithmetic dbms clickhouse_functions)

add_executable (logical_functions_performance logical_functions_performance.cpp)
target_link_libraries (logical_functions_performance dbms)
