clickhouse_add_executable(zkutil_test_commands zkutil_test_commands.cpp)
target_link_libraries(zkutil_test_commands PRIVATE
    clickhouse_common_zookeeper_no_log
    dbms)

clickhouse_add_executable(zkutil_test_commands_new_lib zkutil_test_commands_new_lib.cpp)
target_link_libraries(zkutil_test_commands_new_lib PRIVATE
    clickhouse_common_zookeeper_no_log
    clickhouse_compression
    dbms)

clickhouse_add_executable(zkutil_test_async zkutil_test_async.cpp)
target_link_libraries(zkutil_test_async PRIVATE
    clickhouse_common_zookeeper_no_log
    dbms)
