add_executable (expression expression.cpp)
target_link_libraries (expression dbms)

add_executable (create_query create_query.cpp)
target_link_libraries (create_query dbms)

add_executable (select_query select_query.cpp)
target_link_libraries (select_query clickhouse_storages_system dbms)

add_executable (aggregate aggregate.cpp)
target_link_libraries (aggregate dbms)

add_executable (hash_map hash_map.cpp)
target_include_directories (hash_map SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map dbms)

add_executable (hash_map3 hash_map3.cpp)
target_link_libraries (hash_map3 dbms ${FARMHASH_LIBRARIES} ${METROHASH_LIBRARIES})

add_executable (hash_map_string hash_map_string.cpp)
target_include_directories (hash_map_string SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map_string dbms)

add_executable (hash_map_string_2 hash_map_string_2.cpp)
target_link_libraries (hash_map_string_2 dbms)

add_executable (hash_map_string_3 hash_map_string_3.cpp)
target_link_libraries (hash_map_string_3 dbms ${FARMHASH_LIBRARIES} ${METROHASH_LIBRARIES})

add_executable (hash_map_string_small hash_map_string_small.cpp)
target_include_directories (hash_map_string_small SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map_string_small dbms)

add_executable (two_level_hash_map two_level_hash_map.cpp)
target_include_directories (two_level_hash_map SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (two_level_hash_map dbms)

add_executable (compiler_test compiler_test.cpp)
target_link_libraries (compiler_test dbms)

add_executable (logical_expressions_optimizer logical_expressions_optimizer.cpp)
target_link_libraries (logical_expressions_optimizer dbms)

add_executable (in_join_subqueries_preprocessor in_join_subqueries_preprocessor.cpp)
target_link_libraries (in_join_subqueries_preprocessor dbms)
add_check(in_join_subqueries_preprocessor)

add_executable (users users.cpp)
target_link_libraries (users dbms ${Boost_FILESYSTEM_LIBRARY})

add_executable (internal_iotop internal_iotop.cpp)
target_link_libraries (internal_iotop dbms)
