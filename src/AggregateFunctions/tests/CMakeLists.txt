add_executable (quantile-t-digest quantile-t-digest.cpp)
target_link_libraries (quantile-t-digest PRIVATE dbms clickhouse_aggregate_functions)
