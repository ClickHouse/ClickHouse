add_executable (string_pool string_pool.cpp)
target_link_libraries (string_pool PRIVATE clickhouse_common_io)
target_include_directories (string_pool SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})

add_executable (field field.cpp)
target_link_libraries (field PRIVATE dbms)

add_executable (string_ref_hash string_ref_hash.cpp)
target_link_libraries (string_ref_hash PRIVATE clickhouse_common_io)

add_executable (mysql_protocol mysql_protocol.cpp)
target_link_libraries (mysql_protocol PRIVATE dbms)
if(USE_SSL)
    target_include_directories (mysql_protocol SYSTEM PRIVATE ${OPENSSL_INCLUDE_DIR})
endif()

add_executable (coro coro.cpp)
target_link_libraries (coro PRIVATE clickhouse_common_io)
