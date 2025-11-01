clickhouse_add_executable (names_and_types_fuzzer names_and_types_fuzzer.cpp)
target_link_libraries (names_and_types_fuzzer PRIVATE dbms)

# this is a test case for specific PR https://github.com/ClickHouse/ClickHouse/pull/89063
# should be removed after testing and not appear in master
clickhouse_add_executable (ci_libfuzzer_test_fuzzer ci_libfuzzer_test_fuzzer.cpp)
