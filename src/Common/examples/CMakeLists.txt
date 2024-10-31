clickhouse_add_executable (hashes_test hashes_test.cpp)
target_link_libraries (hashes_test PRIVATE clickhouse_common_io clickhouse_common_config ch_contrib::cityhash)
if (TARGET OpenSSL::Crypto)
    target_link_libraries (hashes_test PRIVATE OpenSSL::Crypto)
endif()

clickhouse_add_executable (sip_hash_perf sip_hash_perf.cpp)
target_link_libraries (sip_hash_perf PRIVATE clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (small_table small_table.cpp)
target_link_libraries (small_table PRIVATE clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (parallel_aggregation parallel_aggregation.cpp)
target_link_libraries (parallel_aggregation PRIVATE dbms)

clickhouse_add_executable (parallel_aggregation2 parallel_aggregation2.cpp)
target_link_libraries (parallel_aggregation2 PRIVATE dbms)

clickhouse_add_executable (int_hashes_perf int_hashes_perf.cpp)
target_link_libraries (int_hashes_perf PRIVATE clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (compact_array compact_array.cpp)
target_link_libraries (compact_array PRIVATE clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (radix_sort radix_sort.cpp)
target_link_libraries (radix_sort PRIVATE clickhouse_common_io clickhouse_common_config ch_contrib::pdqsort)

clickhouse_add_executable (arena_with_free_lists arena_with_free_lists.cpp)
target_link_libraries (arena_with_free_lists PRIVATE dbms)

clickhouse_add_executable (lru_hash_map_perf lru_hash_map_perf.cpp)
target_link_libraries (lru_hash_map_perf PRIVATE dbms)

if (OS_LINUX)
    clickhouse_add_executable (thread_creation_latency thread_creation_latency.cpp)
    target_link_libraries (thread_creation_latency PRIVATE dbms clickhouse_common_io clickhouse_common_config)
endif()

clickhouse_add_executable (array_cache array_cache.cpp)
target_link_libraries (array_cache PRIVATE clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (space_saving space_saving.cpp)
target_link_libraries (space_saving PRIVATE dbms clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (integer_hash_tables_benchmark integer_hash_tables_benchmark.cpp)
target_link_libraries (integer_hash_tables_benchmark PRIVATE dbms ch_contrib::abseil_swiss_tables ch_contrib::sparsehash)

clickhouse_add_executable (cow_columns cow_columns.cpp)
target_link_libraries (cow_columns PRIVATE clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (cow_compositions cow_compositions.cpp)
target_link_libraries (cow_compositions PRIVATE clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (stopwatch stopwatch.cpp)
target_link_libraries (stopwatch PRIVATE clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (symbol_index symbol_index.cpp)
target_link_libraries (symbol_index PRIVATE clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (chaos_sanitizer chaos_sanitizer.cpp)
target_link_libraries (chaos_sanitizer PRIVATE clickhouse_common_io clickhouse_common_config)

if (OS_LINUX)
    clickhouse_add_executable (memory_statistics_os_perf memory_statistics_os_perf.cpp)
    target_link_libraries (memory_statistics_os_perf PRIVATE clickhouse_common_io clickhouse_common_config)
endif()

clickhouse_add_executable (procfs_metrics_provider_perf procfs_metrics_provider_perf.cpp)
target_link_libraries (procfs_metrics_provider_perf PRIVATE clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (average average.cpp)
target_link_libraries (average PRIVATE dbms clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (shell_command_inout shell_command_inout.cpp)
target_link_libraries (shell_command_inout PRIVATE clickhouse_common_io clickhouse_common_config)

clickhouse_add_executable (executable_udf executable_udf.cpp)
target_link_libraries (executable_udf PRIVATE dbms)

if (ENABLE_HIVE)
    clickhouse_add_executable (hive_metastore_client hive_metastore_client.cpp)
    target_link_libraries (hive_metastore_client PUBLIC ch_contrib::hivemetastore ch_contrib::thrift)
endif()

clickhouse_add_executable (interval_tree interval_tree.cpp)
target_link_libraries (interval_tree PRIVATE dbms)

if (ENABLE_SSL)
    clickhouse_add_executable (encrypt_decrypt encrypt_decrypt.cpp)
    target_link_libraries (encrypt_decrypt PRIVATE dbms)
endif()

clickhouse_add_executable (check_pointer_valid check_pointer_valid.cpp)
target_link_libraries (check_pointer_valid PRIVATE clickhouse_common_io clickhouse_common_config)

if (TARGET ch_contrib::icu)
    clickhouse_add_executable (utf8_upper_lower utf8_upper_lower.cpp)
    target_link_libraries (utf8_upper_lower PRIVATE ch_contrib::icu)
endif ()
