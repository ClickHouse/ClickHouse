clickhouse_add_executable(test-remote-disk test_remote_disk.cpp)
target_link_libraries (test-remote-disk PRIVATE dbms)