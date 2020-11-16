add_executable (string_pool string_pool.cpp)
target_link_libraries (string_pool PRIVATE clickhouse_common_io)
target_include_directories (string_pool SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})

add_executable (field field.cpp)
target_link_libraries (field PRIVATE dbms)

add_executable (string_ref_hash string_ref_hash.cpp)
target_link_libraries (string_ref_hash PRIVATE clickhouse_common_io)

if (ENABLE_FUZZING)
    add_executable (names_and_types_fuzzer names_and_types_fuzzer.cpp)
    target_link_libraries (names_and_types_fuzzer PRIVATE dbms ${LIB_FUZZING_ENGINE})
endif ()

add_executable (mysql_protocol mysql_protocol.cpp)
target_link_libraries (mysql_protocol PRIVATE dbms)
if(USE_SSL)
    target_include_directories (mysql_protocol SYSTEM PRIVATE ${OPENSSL_INCLUDE_DIR})
endif()
