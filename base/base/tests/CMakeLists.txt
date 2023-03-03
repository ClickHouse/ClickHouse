add_executable (dump_variable dump_variable.cpp)
target_link_libraries (dump_variable PRIVATE clickhouse_common_io)
