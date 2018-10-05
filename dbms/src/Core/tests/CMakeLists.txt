add_executable (exception exception.cpp)
target_link_libraries (exception clickhouse_common_io)

add_executable (string_pool string_pool.cpp)
target_link_libraries (string_pool clickhouse_common_io)
target_include_directories (string_pool SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})

add_executable (field field.cpp)
target_link_libraries (field dbms)

add_executable (move_field move_field.cpp)
target_link_libraries (move_field clickhouse_common_io)

add_executable (rvo_test rvo_test.cpp)
target_link_libraries (rvo_test Threads::Threads)

add_executable (string_ref_hash string_ref_hash.cpp)
target_link_libraries (string_ref_hash clickhouse_common_io)
