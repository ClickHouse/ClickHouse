add_executable (hashes_test hashes_test.cpp)
target_link_libraries (hashes_test dbms)

add_executable (sip_hash sip_hash.cpp)
target_link_libraries (sip_hash dbms)

add_executable (sip_hash_perf sip_hash_perf.cpp)
target_link_libraries (sip_hash_perf dbms)

add_executable (auto_array auto_array.cpp)
target_link_libraries (auto_array dbms)

add_executable (lru_cache lru_cache.cpp)
target_link_libraries (lru_cache dbms)

add_executable (hash_table hash_table.cpp)
target_link_libraries (hash_table dbms)

add_executable (small_table small_table.cpp)
target_link_libraries (small_table dbms)

add_executable (parallel_aggregation parallel_aggregation.cpp)
target_link_libraries (parallel_aggregation dbms)

add_executable (parallel_aggregation2 parallel_aggregation2.cpp)
target_link_libraries (parallel_aggregation2 dbms)

add_executable (int_hashes_perf int_hashes_perf.cpp AvalancheTest.cpp Random.cpp)
target_link_libraries (int_hashes_perf dbms)

add_executable (simple_cache simple_cache.cpp)
target_link_libraries (simple_cache common)

add_executable (compact_array compact_array.cpp)
target_link_libraries (compact_array dbms ${Boost_FILESYSTEM_LIBRARY})

add_executable (radix_sort radix_sort.cpp)
target_link_libraries (radix_sort dbms)

add_executable (shell_command_test shell_command_test.cpp)
target_link_libraries (shell_command_test dbms)

add_executable (arena_with_free_lists arena_with_free_lists.cpp)
target_link_libraries (arena_with_free_lists dbms)

add_executable (pod_array pod_array.cpp)
target_link_libraries (pod_array dbms)

add_executable (thread_creation_latency thread_creation_latency.cpp)
target_link_libraries (thread_creation_latency dbms)

add_executable (thread_pool thread_pool.cpp)
target_link_libraries (thread_pool dbms)

add_executable (array_cache array_cache.cpp)
target_link_libraries (array_cache dbms)

add_executable (space_saving space_saving.cpp)
target_link_libraries (space_saving dbms)
