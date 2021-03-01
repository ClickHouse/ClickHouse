add_executable (mercator mercator.cpp)
target_link_libraries (mercator PRIVATE clickhouse_functions)
