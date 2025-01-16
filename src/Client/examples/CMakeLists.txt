clickhouse_add_executable(test-connect test_connect.cpp)
target_link_libraries (test-connect PRIVATE dbms)

clickhouse_add_executable(preprocess-for-autocomplete preprocess_for_autocomplete.cpp)
target_link_libraries (preprocess-for-autocomplete PRIVATE dbms)
