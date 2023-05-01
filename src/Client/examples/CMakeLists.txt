clickhouse_add_executable(test-connect test_connect.cpp)
target_link_libraries (test-connect PRIVATE dbms)

clickhouse_add_executable(test-remote-fs-connect test_remote_fs_connect.cpp)
target_link_libraries (test-remote-fs-connect PRIVATE dbms)
