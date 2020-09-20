add_executable (hashes_test hashes_test.cpp)
target_link_libraries (hashes_test PRIVATE clickhouse_common_io ${CITYHASH_LIBRARIES})
if(OPENSSL_CRYPTO_LIBRARY)
    target_link_libraries (hashes_test PRIVATE ${OPENSSL_CRYPTO_LIBRARY})
endif()

add_executable (sip_hash_perf sip_hash_perf.cpp)
target_link_libraries (sip_hash_perf PRIVATE clickhouse_common_io)

add_executable (auto_array auto_array.cpp)
target_link_libraries (auto_array PRIVATE clickhouse_common_io)

add_executable (hash_table hash_table.cpp)
target_link_libraries (hash_table PRIVATE clickhouse_common_io)

add_executable (small_table small_table.cpp)
target_link_libraries (small_table PRIVATE clickhouse_common_io)

add_executable (parallel_aggregation parallel_aggregation.cpp)
target_link_libraries (parallel_aggregation PRIVATE dbms)

add_executable (parallel_aggregation2 parallel_aggregation2.cpp)
target_link_libraries (parallel_aggregation2 PRIVATE dbms)

add_executable (int_hashes_perf int_hashes_perf.cpp)
target_link_libraries (int_hashes_perf PRIVATE clickhouse_common_io)

add_executable (simple_cache simple_cache.cpp)
target_link_libraries (simple_cache PRIVATE common)

add_executable (compact_array compact_array.cpp)
target_link_libraries (compact_array PRIVATE clickhouse_common_io)

add_executable (radix_sort radix_sort.cpp)
target_link_libraries (radix_sort PRIVATE clickhouse_common_io)
target_include_directories(radix_sort SYSTEM PRIVATE ${PDQSORT_INCLUDE_DIR})

add_executable (arena_with_free_lists arena_with_free_lists.cpp)
target_link_libraries (arena_with_free_lists PRIVATE dbms)

add_executable (pod_array pod_array.cpp)
target_link_libraries (pod_array PRIVATE clickhouse_common_io)

add_executable (thread_creation_latency thread_creation_latency.cpp)
target_link_libraries (thread_creation_latency PRIVATE clickhouse_common_io)

add_executable (array_cache array_cache.cpp)
target_link_libraries (array_cache PRIVATE clickhouse_common_io)

add_executable (space_saving space_saving.cpp)
target_link_libraries (space_saving PRIVATE clickhouse_common_io)

add_executable (integer_hash_tables_and_hashes integer_hash_tables_and_hashes.cpp)
target_include_directories (integer_hash_tables_and_hashes SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})
target_link_libraries (integer_hash_tables_and_hashes PRIVATE dbms)

add_executable (cow_columns cow_columns.cpp)
target_link_libraries (cow_columns PRIVATE clickhouse_common_io)

add_executable (cow_compositions cow_compositions.cpp)
target_link_libraries (cow_compositions PRIVATE clickhouse_common_io)

add_executable (stopwatch stopwatch.cpp)
target_link_libraries (stopwatch PRIVATE clickhouse_common_io)

add_executable (symbol_index symbol_index.cpp)
target_link_libraries (symbol_index PRIVATE clickhouse_common_io)

add_executable (chaos_sanitizer chaos_sanitizer.cpp)
target_link_libraries (chaos_sanitizer PRIVATE clickhouse_common_io)

if (OS_LINUX)
    add_executable (memory_statistics_os_perf memory_statistics_os_perf.cpp)
    target_link_libraries (memory_statistics_os_perf PRIVATE clickhouse_common_io)
endif()

add_executable (procfs_metrics_provider_perf procfs_metrics_provider_perf.cpp)
target_link_libraries (procfs_metrics_provider_perf PRIVATE clickhouse_common_io)

add_executable (average average.cpp)
target_link_libraries (average PRIVATE clickhouse_common_io)

add_executable (shell_command_inout shell_command_inout.cpp)
target_link_libraries (shell_command_inout PRIVATE clickhouse_common_io)
