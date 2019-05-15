add_executable (expression expression.cpp)
target_link_libraries (expression PRIVATE dbms clickhouse_parsers)

add_executable (create_query create_query.cpp)
target_link_libraries (create_query PRIVATE dbms clickhouse_parsers)

add_executable (select_query select_query.cpp)
target_link_libraries (select_query PRIVATE clickhouse_storages_system dbms clickhouse_common_io)

add_executable (aggregate aggregate.cpp)
target_link_libraries (aggregate PRIVATE dbms)

add_executable (hash_map hash_map.cpp)
target_include_directories (hash_map SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map PRIVATE dbms clickhouse_compression)

add_executable (hash_map_lookup hash_map_lookup.cpp)
target_include_directories (hash_map_lookup SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map_lookup PRIVATE dbms clickhouse_compression)

add_executable (hash_map3 hash_map3.cpp)
target_include_directories(hash_map3 SYSTEM BEFORE PRIVATE ${METROHASH_INCLUDE_DIR})
target_link_libraries (hash_map3 PRIVATE dbms ${FARMHASH_LIBRARIES} ${METROHASH_LIBRARIES})

add_executable (hash_map_string hash_map_string.cpp)
target_include_directories (hash_map_string SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map_string PRIVATE dbms clickhouse_compression)

add_executable (hash_map_string_2 hash_map_string_2.cpp)
target_link_libraries (hash_map_string_2 PRIVATE dbms clickhouse_compression)

add_executable (hash_map_string_3 hash_map_string_3.cpp)
target_include_directories(hash_map_string_3 SYSTEM BEFORE PRIVATE ${METROHASH_INCLUDE_DIR})
target_link_libraries (hash_map_string_3 PRIVATE dbms clickhouse_compression ${FARMHASH_LIBRARIES} ${METROHASH_LIBRARIES})

add_executable (hash_map_string_small hash_map_string_small.cpp)
target_include_directories (hash_map_string_small SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map_string_small PRIVATE dbms clickhouse_compression)

add_executable (two_level_hash_map two_level_hash_map.cpp)
target_include_directories (two_level_hash_map SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (two_level_hash_map PRIVATE dbms clickhouse_compression)

add_executable (compiler_test compiler_test.cpp)
target_link_libraries (compiler_test PRIVATE dbms)

add_executable (logical_expressions_optimizer logical_expressions_optimizer.cpp)
target_link_libraries (logical_expressions_optimizer PRIVATE dbms clickhouse_parsers)

add_executable (in_join_subqueries_preprocessor in_join_subqueries_preprocessor.cpp)
target_link_libraries (in_join_subqueries_preprocessor PRIVATE dbms clickhouse_parsers)
add_check(in_join_subqueries_preprocessor)

add_executable (expression_analyzer expression_analyzer.cpp)
target_link_libraries (expression_analyzer PRIVATE dbms clickhouse_storages_system clickhouse_parsers clickhouse_common_io)
add_check(expression_analyzer)

add_executable (users users.cpp)
target_link_libraries (users PRIVATE dbms clickhouse_common_config ${Boost_FILESYSTEM_LIBRARY})

if (OS_LINUX)
    add_executable (internal_iotop internal_iotop.cpp)
    target_link_libraries (internal_iotop PRIVATE dbms Threads::Threads)
endif ()
