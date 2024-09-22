clickhouse_add_executable(test-connect test_connect.cpp)
target_link_libraries (test-connect PRIVATE dbms clickhouse_functions)
