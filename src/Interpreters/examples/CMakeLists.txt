add_executable (hash_map hash_map.cpp)
target_include_directories (hash_map SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})
target_link_libraries (hash_map PRIVATE dbms)

add_executable (hash_map_lookup hash_map_lookup.cpp)
target_include_directories (hash_map_lookup SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})
target_link_libraries (hash_map_lookup PRIVATE dbms)

add_executable (hash_map3 hash_map3.cpp)
target_link_libraries (hash_map3 PRIVATE dbms ${FARMHASH_LIBRARIES} metrohash)

add_executable (hash_map_string hash_map_string.cpp)
target_include_directories (hash_map_string SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})
target_link_libraries (hash_map_string PRIVATE dbms)

add_executable (hash_map_string_2 hash_map_string_2.cpp)
target_link_libraries (hash_map_string_2 PRIVATE dbms)

add_executable (hash_map_string_3 hash_map_string_3.cpp)
target_link_libraries (hash_map_string_3 PRIVATE dbms ${FARMHASH_LIBRARIES} metrohash)

add_executable (hash_map_string_small hash_map_string_small.cpp)
target_include_directories (hash_map_string_small SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})
target_link_libraries (hash_map_string_small PRIVATE dbms)

add_executable (string_hash_map string_hash_map.cpp)
target_link_libraries (string_hash_map PRIVATE dbms)

add_executable (string_hash_map_aggregation string_hash_map.cpp)
target_link_libraries (string_hash_map_aggregation PRIVATE dbms)

add_executable (string_hash_set string_hash_set.cpp)
target_link_libraries (string_hash_set PRIVATE dbms)

add_executable (two_level_hash_map two_level_hash_map.cpp)
target_include_directories (two_level_hash_map SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})
target_link_libraries (two_level_hash_map PRIVATE dbms)

add_executable (jit_example jit_example.cpp)
target_link_libraries (jit_example PRIVATE dbms)
