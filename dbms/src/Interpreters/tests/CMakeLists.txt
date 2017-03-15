add_executable (expression expression.cpp)
target_link_libraries (expression dbms)

add_executable (expression_analyzer expression_analyzer.cpp)
target_link_libraries (expression_analyzer dbms)

add_executable (create_query create_query.cpp)
target_link_libraries (create_query dbms)

add_executable (select_query select_query.cpp)
target_link_libraries (select_query dbms storages_system)

add_executable (aggregate aggregate.cpp)
target_link_libraries (aggregate dbms)

add_executable (hash_map hash_map.cpp)
target_link_libraries (hash_map dbms)

add_executable (hash_map2 hash_map2.cpp)
target_link_libraries (hash_map2 dbms)

add_executable (hash_map3 hash_map3.cpp)
target_link_libraries (hash_map3 dbms)

add_executable (hash_map_string hash_map_string.cpp)
target_link_libraries (hash_map_string dbms)

add_executable (hash_map_string_2 hash_map_string_2.cpp)
target_link_libraries (hash_map_string_2 dbms)

add_executable (hash_map_string_3 hash_map_string_3.cpp)
target_link_libraries (hash_map_string_3 dbms)

add_executable (hash_map_string_small hash_map_string_small.cpp)
target_link_libraries (hash_map_string_small dbms)

add_executable (two_level_hash_map two_level_hash_map.cpp)
target_link_libraries (two_level_hash_map dbms)

add_executable (address_patterns address_patterns.cpp)
target_link_libraries (address_patterns dbms)

add_executable (compiler_test compiler_test.cpp)
target_link_libraries (compiler_test dbms)

add_executable (logical_expressions_optimizer logical_expressions_optimizer.cpp)
target_link_libraries (logical_expressions_optimizer dbms)

add_executable (in_join_subqueries_preprocessor in_join_subqueries_preprocessor.cpp)
target_link_libraries (in_join_subqueries_preprocessor dbms)
add_check(in_join_subqueries_preprocessor)

add_executable (users users.cpp)
target_link_libraries (users dbms ${Boost_FILESYSTEM_LIBRARY})
