add_executable(test_fsst gtest_fsst_string_serialization.cpp)
target_link_libraries (test_fsst PRIVATE
    dbms
    clickhouse_common_io
    clickhouse_common_config
)