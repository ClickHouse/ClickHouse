add_executable (hashes_test hashes_test.cpp)
target_link_libraries (hashes_test PRIVATE clickhouse_common_io ${CITYHASH_LIBRARIES})
if(OPENSSL_CRYPTO_LIBRARY)
    target_link_libraries (hashes_test PRIVATE ${OPENSSL_CRYPTO_LIBRARY})
endif()

add_executable (sip_hash sip_hash.cpp)
target_link_libraries (sip_hash PRIVATE clickhouse_common_io)

add_executable (sip_hash_perf sip_hash_perf.cpp)
target_link_libraries (sip_hash_perf PRIVATE clickhouse_common_io)

add_executable (auto_array auto_array.cpp)
target_link_libraries (auto_array PRIVATE clickhouse_common_io)

add_executable (lru_cache lru_cache.cpp)
target_link_libraries (lru_cache PRIVATE clickhouse_common_io)

add_executable (hash_table hash_table.cpp)
target_link_libraries (hash_table PRIVATE clickhouse_common_io)

add_executable (small_table small_table.cpp)
target_link_libraries (small_table PRIVATE clickhouse_common_io)

add_executable (parallel_aggregation parallel_aggregation.cpp)
target_link_libraries (parallel_aggregation PRIVATE clickhouse_compression clickhouse_common_io)

add_executable (parallel_aggregation2 parallel_aggregation2.cpp)
target_link_libraries (parallel_aggregation2 PRIVATE clickhouse_compression clickhouse_common_io)

add_executable (int_hashes_perf int_hashes_perf.cpp AvalancheTest.cpp Random.cpp)
target_link_libraries (int_hashes_perf PRIVATE clickhouse_common_io)

add_executable (simple_cache simple_cache.cpp)
target_include_directories (simple_cache PRIVATE ${DBMS_INCLUDE_DIR})
target_link_libraries (simple_cache PRIVATE common)

add_executable (compact_array compact_array.cpp)
target_link_libraries (compact_array PRIVATE clickhouse_common_io ${Boost_FILESYSTEM_LIBRARY})

add_executable (radix_sort radix_sort.cpp)
target_link_libraries (radix_sort PRIVATE clickhouse_common_io)

add_executable (shell_command_test shell_command_test.cpp)
target_link_libraries (shell_command_test PRIVATE clickhouse_common_io)

add_executable (arena_with_free_lists arena_with_free_lists.cpp)
target_link_libraries (arena_with_free_lists PRIVATE clickhouse_compression clickhouse_common_io)

add_executable (pod_array pod_array.cpp)
target_link_libraries (pod_array PRIVATE clickhouse_common_io)

add_executable (thread_creation_latency thread_creation_latency.cpp)
target_link_libraries (thread_creation_latency PRIVATE clickhouse_common_io)

add_executable (thread_pool thread_pool.cpp)
target_link_libraries (thread_pool PRIVATE clickhouse_common_io)

add_executable (thread_pool_2 thread_pool_2.cpp)
target_link_libraries (thread_pool_2 PRIVATE clickhouse_common_io)

add_executable (thread_pool_3 thread_pool_3.cpp)
target_link_libraries (thread_pool_3 PRIVATE clickhouse_common_io)

add_executable (multi_version multi_version.cpp)
target_link_libraries (multi_version PRIVATE clickhouse_common_io)
add_check(multi_version)

add_executable (array_cache array_cache.cpp)
target_link_libraries (array_cache PRIVATE clickhouse_common_io)

add_executable (space_saving space_saving.cpp)
target_link_libraries (space_saving PRIVATE clickhouse_common_io)

add_executable (integer_hash_tables_and_hashes integer_hash_tables_and_hashes.cpp)
target_include_directories (integer_hash_tables_and_hashes SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (integer_hash_tables_and_hashes PRIVATE clickhouse_compression clickhouse_common_io)

add_executable (allocator allocator.cpp)
target_link_libraries (allocator PRIVATE clickhouse_common_io)

add_executable (cow_columns cow_columns.cpp)
target_link_libraries (cow_columns PRIVATE clickhouse_common_io)

add_executable (cow_compositions cow_compositions.cpp)
target_link_libraries (cow_compositions PRIVATE clickhouse_common_io)

add_executable (stopwatch stopwatch.cpp)
target_link_libraries (stopwatch PRIVATE clickhouse_common_io)
