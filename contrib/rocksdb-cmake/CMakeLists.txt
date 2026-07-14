option (ENABLE_ROCKSDB "Enable RocksDB" ${ENABLE_LIBRARIES})

if (NOT ENABLE_ROCKSDB OR NO_SSE3_OR_HIGHER) # assumes SSE4.2 and PCLMUL
  message (STATUS "Not using RocksDB")
  return()
endif()

# ClickHouse cannot be compiled without snappy, lz4, zlib, zstd
option(WITH_SNAPPY "build with SNAPPY" ON)
option(WITH_LZ4 "build with lz4" ON)
option(WITH_ZLIB "build with zlib" ON)
option(WITH_ZSTD "build with zstd" ON)

if (ENABLE_JEMALLOC AND OS_LINUX) # gives compile errors with jemalloc enabled for rocksdb on non-Linux
  add_definitions(-DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE)
  list (APPEND THIRDPARTY_LIBS ch_contrib::jemalloc)
endif ()

if (ENABLE_LIBURING)
  add_definitions(-DROCKSDB_IOURING_PRESENT)
  list (APPEND THIRDPARTY_LIBS ch_contrib::liburing)
endif ()

if (WITH_SNAPPY)
  add_definitions(-DSNAPPY)
  list(APPEND THIRDPARTY_LIBS ch_contrib::snappy)
endif()

if (WITH_ZLIB)
  add_definitions(-DZLIB)
  list(APPEND THIRDPARTY_LIBS ch_contrib::zlib)
endif()

if (WITH_LZ4)
  add_definitions(-DLZ4)
  list(APPEND THIRDPARTY_LIBS ch_contrib::lz4)
endif()

if (WITH_ZSTD)
  add_definitions(-DZSTD)
  list(APPEND THIRDPARTY_LIBS ch_contrib::zstd)
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64|AARCH64")
  set (HAS_ARMV8_CRC 1)
  # the original build descriptions set specific flags for ARM. These flags are already subsumed by ClickHouse's general
  # ARM flags, see cmake/cpu_features.cmake
  # set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=armv8-a+crc+crypto -Wno-unused-function")
  # set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv8-a+crc+crypto -Wno-unused-function")
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  add_definitions(-DOS_MACOSX)
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
  add_definitions(-DOS_LINUX)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  add_definitions(-DOS_FREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "Android")
  add_definitions(-DOS_ANDROID)
endif()

add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)

if ((OS_LINUX OR OS_FREEBSD) AND NOT USE_MUSL)
  add_definitions(-DROCKSDB_PTHREAD_ADAPTIVE_MUTEX)
endif()

if (OS_LINUX)
  add_definitions(-DROCKSDB_SCHED_GETCPU_PRESENT)
  add_definitions(-DROCKSDB_AUXV_GETAUXVAL_PRESENT)
endif()

set(ROCKSDB_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/rocksdb")

include_directories(${ROCKSDB_SOURCE_DIR})
include_directories("${ROCKSDB_SOURCE_DIR}/include")

set(SOURCES
    ${ROCKSDB_SOURCE_DIR}/cache/cache.cc
    ${ROCKSDB_SOURCE_DIR}/cache/cache_entry_roles.cc
    ${ROCKSDB_SOURCE_DIR}/cache/cache_key.cc
    ${ROCKSDB_SOURCE_DIR}/cache/cache_helpers.cc
    ${ROCKSDB_SOURCE_DIR}/cache/cache_reservation_manager.cc
    ${ROCKSDB_SOURCE_DIR}/cache/charged_cache.cc
    ${ROCKSDB_SOURCE_DIR}/cache/clock_cache.cc
    ${ROCKSDB_SOURCE_DIR}/cache/compressed_secondary_cache.cc
    ${ROCKSDB_SOURCE_DIR}/cache/lru_cache.cc
    ${ROCKSDB_SOURCE_DIR}/cache/secondary_cache.cc
    ${ROCKSDB_SOURCE_DIR}/cache/secondary_cache_adapter.cc
    ${ROCKSDB_SOURCE_DIR}/cache/sharded_cache.cc
    ${ROCKSDB_SOURCE_DIR}/cache/tiered_secondary_cache.cc
    ${ROCKSDB_SOURCE_DIR}/db/arena_wrapped_db_iter.cc
    ${ROCKSDB_SOURCE_DIR}/db/attribute_group_iterator_impl.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_contents.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_fetcher.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_file_addition.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_file_builder.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_file_cache.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_file_garbage.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_file_meta.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_file_reader.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_garbage_meter.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_log_format.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_log_sequential_reader.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_log_writer.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/blob_source.cc
    ${ROCKSDB_SOURCE_DIR}/db/blob/prefetch_buffer_collection.cc
    ${ROCKSDB_SOURCE_DIR}/db/builder.cc
    ${ROCKSDB_SOURCE_DIR}/db/c.cc
    ${ROCKSDB_SOURCE_DIR}/db/coalescing_iterator.cc
    ${ROCKSDB_SOURCE_DIR}/db/column_family.cc
    ${ROCKSDB_SOURCE_DIR}/db/compaction/compaction.cc
    ${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_iterator.cc
    ${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_picker.cc
    ${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_job.cc
    ${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_picker_fifo.cc
    ${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_picker_level.cc
    ${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_picker_universal.cc
    ${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_service_job.cc
    ${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_state.cc
    ${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_outputs.cc
    ${ROCKSDB_SOURCE_DIR}/db/compaction/sst_partitioner.cc
    ${ROCKSDB_SOURCE_DIR}/db/compaction/subcompaction_state.cc
    ${ROCKSDB_SOURCE_DIR}/db/convenience.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_filesnapshot.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_impl/compacted_db_impl.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_write.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_compaction_flush.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_files.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_follower.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_open.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_experimental.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_readonly.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_secondary.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_info_dumper.cc
    ${ROCKSDB_SOURCE_DIR}/db/db_iter.cc
    ${ROCKSDB_SOURCE_DIR}/db/dbformat.cc
    ${ROCKSDB_SOURCE_DIR}/db/error_handler.cc
    ${ROCKSDB_SOURCE_DIR}/db/event_helpers.cc
    ${ROCKSDB_SOURCE_DIR}/db/external_sst_file_ingestion_job.cc
    ${ROCKSDB_SOURCE_DIR}/db/file_indexer.cc
    ${ROCKSDB_SOURCE_DIR}/db/flush_job.cc
    ${ROCKSDB_SOURCE_DIR}/db/flush_scheduler.cc
    ${ROCKSDB_SOURCE_DIR}/db/forward_iterator.cc
    ${ROCKSDB_SOURCE_DIR}/db/import_column_family_job.cc
    ${ROCKSDB_SOURCE_DIR}/db/internal_stats.cc
    ${ROCKSDB_SOURCE_DIR}/db/logs_with_prep_tracker.cc
    ${ROCKSDB_SOURCE_DIR}/db/log_reader.cc
    ${ROCKSDB_SOURCE_DIR}/db/log_writer.cc
    ${ROCKSDB_SOURCE_DIR}/db/malloc_stats.cc
    ${ROCKSDB_SOURCE_DIR}/db/memtable.cc
    ${ROCKSDB_SOURCE_DIR}/db/memtable_list.cc
    ${ROCKSDB_SOURCE_DIR}/db/merge_helper.cc
    ${ROCKSDB_SOURCE_DIR}/db/merge_operator.cc
    ${ROCKSDB_SOURCE_DIR}/db/output_validator.cc
    ${ROCKSDB_SOURCE_DIR}/db/periodic_task_scheduler.cc
    ${ROCKSDB_SOURCE_DIR}/db/range_del_aggregator.cc
    ${ROCKSDB_SOURCE_DIR}/db/range_tombstone_fragmenter.cc
    ${ROCKSDB_SOURCE_DIR}/db/repair.cc
    ${ROCKSDB_SOURCE_DIR}/db/seqno_to_time_mapping.cc
    ${ROCKSDB_SOURCE_DIR}/db/snapshot_impl.cc
    ${ROCKSDB_SOURCE_DIR}/db/table_cache.cc
    ${ROCKSDB_SOURCE_DIR}/db/table_properties_collector.cc
    ${ROCKSDB_SOURCE_DIR}/db/transaction_log_impl.cc
    ${ROCKSDB_SOURCE_DIR}/db/trim_history_scheduler.cc
    ${ROCKSDB_SOURCE_DIR}/db/version_builder.cc
    ${ROCKSDB_SOURCE_DIR}/db/version_edit.cc
    ${ROCKSDB_SOURCE_DIR}/db/version_edit_handler.cc
    ${ROCKSDB_SOURCE_DIR}/db/version_set.cc
    ${ROCKSDB_SOURCE_DIR}/db/wal_edit.cc
    ${ROCKSDB_SOURCE_DIR}/db/wal_manager.cc
    ${ROCKSDB_SOURCE_DIR}/db/wide/wide_column_serialization.cc
    ${ROCKSDB_SOURCE_DIR}/db/wide/wide_columns.cc
    ${ROCKSDB_SOURCE_DIR}/db/wide/wide_columns_helper.cc
    ${ROCKSDB_SOURCE_DIR}/db/write_batch.cc
    ${ROCKSDB_SOURCE_DIR}/db/write_batch_base.cc
    ${ROCKSDB_SOURCE_DIR}/db/write_controller.cc
    ${ROCKSDB_SOURCE_DIR}/db/write_stall_stats.cc
    ${ROCKSDB_SOURCE_DIR}/db/write_thread.cc
    ${ROCKSDB_SOURCE_DIR}/env/composite_env.cc
    ${ROCKSDB_SOURCE_DIR}/env/env.cc
    ${ROCKSDB_SOURCE_DIR}/env/env_chroot.cc
    ${ROCKSDB_SOURCE_DIR}/env/env_encryption.cc
    ${ROCKSDB_SOURCE_DIR}/env/file_system.cc
    ${ROCKSDB_SOURCE_DIR}/env/file_system_tracer.cc
    ${ROCKSDB_SOURCE_DIR}/env/fs_on_demand.cc
    ${ROCKSDB_SOURCE_DIR}/env/fs_remap.cc
    ${ROCKSDB_SOURCE_DIR}/env/mock_env.cc
    ${ROCKSDB_SOURCE_DIR}/env/unique_id_gen.cc
    ${ROCKSDB_SOURCE_DIR}/file/delete_scheduler.cc
    ${ROCKSDB_SOURCE_DIR}/file/file_prefetch_buffer.cc
    ${ROCKSDB_SOURCE_DIR}/file/file_util.cc
    ${ROCKSDB_SOURCE_DIR}/file/filename.cc
    ${ROCKSDB_SOURCE_DIR}/file/line_file_reader.cc
    ${ROCKSDB_SOURCE_DIR}/file/random_access_file_reader.cc
    ${ROCKSDB_SOURCE_DIR}/file/read_write_util.cc
    ${ROCKSDB_SOURCE_DIR}/file/readahead_raf.cc
    ${ROCKSDB_SOURCE_DIR}/file/sequence_file_reader.cc
    ${ROCKSDB_SOURCE_DIR}/file/sst_file_manager_impl.cc
    ${ROCKSDB_SOURCE_DIR}/file/writable_file_writer.cc
    ${ROCKSDB_SOURCE_DIR}/logging/auto_roll_logger.cc
    ${ROCKSDB_SOURCE_DIR}/logging/event_logger.cc
    ${ROCKSDB_SOURCE_DIR}/logging/log_buffer.cc
    ${ROCKSDB_SOURCE_DIR}/memory/arena.cc
    ${ROCKSDB_SOURCE_DIR}/memory/concurrent_arena.cc
    ${ROCKSDB_SOURCE_DIR}/memory/jemalloc_nodump_allocator.cc
    ${ROCKSDB_SOURCE_DIR}/memory/memkind_kmem_allocator.cc
    ${ROCKSDB_SOURCE_DIR}/memory/memory_allocator.cc
    ${ROCKSDB_SOURCE_DIR}/memtable/alloc_tracker.cc
    ${ROCKSDB_SOURCE_DIR}/memtable/hash_linklist_rep.cc
    ${ROCKSDB_SOURCE_DIR}/memtable/hash_skiplist_rep.cc
    ${ROCKSDB_SOURCE_DIR}/memtable/skiplistrep.cc
    ${ROCKSDB_SOURCE_DIR}/memtable/vectorrep.cc
    ${ROCKSDB_SOURCE_DIR}/memtable/write_buffer_manager.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/histogram.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/histogram_windowing.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/in_memory_stats_history.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/instrumented_mutex.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/iostats_context.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/perf_context.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/perf_level.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/persistent_stats_history.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/statistics.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/thread_status_impl.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/thread_status_updater.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/thread_status_util.cc
    ${ROCKSDB_SOURCE_DIR}/monitoring/thread_status_util_debug.cc # Only needed on debug mode
    ${ROCKSDB_SOURCE_DIR}/options/cf_options.cc
    ${ROCKSDB_SOURCE_DIR}/options/configurable.cc
    ${ROCKSDB_SOURCE_DIR}/options/customizable.cc
    ${ROCKSDB_SOURCE_DIR}/options/db_options.cc
    ${ROCKSDB_SOURCE_DIR}/options/offpeak_time_info.cc
    ${ROCKSDB_SOURCE_DIR}/options/options.cc
    ${ROCKSDB_SOURCE_DIR}/options/options_helper.cc
    ${ROCKSDB_SOURCE_DIR}/options/options_parser.cc
    ${ROCKSDB_SOURCE_DIR}/port/mmap.cc
    ${ROCKSDB_SOURCE_DIR}/port/stack_trace.cc
    ${ROCKSDB_SOURCE_DIR}/table/adaptive/adaptive_table_factory.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/binary_search_index_reader.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/block.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/block_based_table_builder.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/block_based_table_factory.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/block_based_table_iterator.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/block_based_table_reader.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/block_builder.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/block_cache.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/block_prefetcher.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/block_prefix_index.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/data_block_footer.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/data_block_hash_index.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/filter_block_reader_common.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/filter_policy.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/flush_block_policy.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/full_filter_block.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/hash_index_reader.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/index_builder.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/index_reader_common.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/parsed_full_filter_block.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/partitioned_filter_block.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/partitioned_index_iterator.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/partitioned_index_reader.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/reader_common.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_based/uncompression_dict_reader.cc
    ${ROCKSDB_SOURCE_DIR}/table/block_fetcher.cc
    ${ROCKSDB_SOURCE_DIR}/table/compaction_merging_iterator.cc
    ${ROCKSDB_SOURCE_DIR}/table/cuckoo/cuckoo_table_builder.cc
    ${ROCKSDB_SOURCE_DIR}/table/cuckoo/cuckoo_table_factory.cc
    ${ROCKSDB_SOURCE_DIR}/table/cuckoo/cuckoo_table_reader.cc
    ${ROCKSDB_SOURCE_DIR}/table/format.cc
    ${ROCKSDB_SOURCE_DIR}/table/get_context.cc
    ${ROCKSDB_SOURCE_DIR}/table/iterator.cc
    ${ROCKSDB_SOURCE_DIR}/table/merging_iterator.cc
    ${ROCKSDB_SOURCE_DIR}/table/meta_blocks.cc
    ${ROCKSDB_SOURCE_DIR}/table/persistent_cache_helper.cc
    ${ROCKSDB_SOURCE_DIR}/table/plain/plain_table_bloom.cc
    ${ROCKSDB_SOURCE_DIR}/table/plain/plain_table_builder.cc
    ${ROCKSDB_SOURCE_DIR}/table/plain/plain_table_factory.cc
    ${ROCKSDB_SOURCE_DIR}/table/plain/plain_table_index.cc
    ${ROCKSDB_SOURCE_DIR}/table/plain/plain_table_key_coding.cc
    ${ROCKSDB_SOURCE_DIR}/table/plain/plain_table_reader.cc
    ${ROCKSDB_SOURCE_DIR}/table/sst_file_dumper.cc
    ${ROCKSDB_SOURCE_DIR}/table/sst_file_reader.cc
    ${ROCKSDB_SOURCE_DIR}/table/sst_file_writer.cc
    ${ROCKSDB_SOURCE_DIR}/table/table_factory.cc
    ${ROCKSDB_SOURCE_DIR}/table/table_properties.cc
    ${ROCKSDB_SOURCE_DIR}/table/two_level_iterator.cc
    ${ROCKSDB_SOURCE_DIR}/table/unique_id.cc
    ${ROCKSDB_SOURCE_DIR}/test_util/sync_point.cc # Only needed on debug mode
    ${ROCKSDB_SOURCE_DIR}/test_util/sync_point_impl.cc # Only needed on debug mode
    ${ROCKSDB_SOURCE_DIR}/test_util/transaction_test_util.cc # Only needed on debug mode
    ${ROCKSDB_SOURCE_DIR}/trace_replay/block_cache_tracer.cc
    ${ROCKSDB_SOURCE_DIR}/trace_replay/io_tracer.cc
    ${ROCKSDB_SOURCE_DIR}/trace_replay/trace_record_handler.cc
    ${ROCKSDB_SOURCE_DIR}/trace_replay/trace_record_result.cc
    ${ROCKSDB_SOURCE_DIR}/trace_replay/trace_record.cc
    ${ROCKSDB_SOURCE_DIR}/trace_replay/trace_replay.cc
    ${ROCKSDB_SOURCE_DIR}/util/async_file_reader.cc
    ${ROCKSDB_SOURCE_DIR}/util/cleanable.cc
    ${ROCKSDB_SOURCE_DIR}/util/coding.cc
    ${ROCKSDB_SOURCE_DIR}/util/compaction_job_stats_impl.cc
    ${ROCKSDB_SOURCE_DIR}/util/comparator.cc
    ${ROCKSDB_SOURCE_DIR}/util/compression.cc
    ${ROCKSDB_SOURCE_DIR}/util/compression_context_cache.cc
    ${ROCKSDB_SOURCE_DIR}/util/concurrent_task_limiter_impl.cc
    ${ROCKSDB_SOURCE_DIR}/util/crc32c.cc
    ${ROCKSDB_SOURCE_DIR}/util/data_structure.cc
    ${ROCKSDB_SOURCE_DIR}/util/dynamic_bloom.cc
    ${ROCKSDB_SOURCE_DIR}/util/hash.cc
    ${ROCKSDB_SOURCE_DIR}/util/murmurhash.cc
    ${ROCKSDB_SOURCE_DIR}/util/random.cc
    ${ROCKSDB_SOURCE_DIR}/util/rate_limiter.cc
    ${ROCKSDB_SOURCE_DIR}/util/ribbon_config.cc
    ${ROCKSDB_SOURCE_DIR}/util/slice.cc
    ${ROCKSDB_SOURCE_DIR}/util/file_checksum_helper.cc
    ${ROCKSDB_SOURCE_DIR}/util/status.cc
    ${ROCKSDB_SOURCE_DIR}/util/stderr_logger.cc
    ${ROCKSDB_SOURCE_DIR}/util/string_util.cc
    ${ROCKSDB_SOURCE_DIR}/util/thread_local.cc
    ${ROCKSDB_SOURCE_DIR}/util/threadpool_imp.cc
    ${ROCKSDB_SOURCE_DIR}/util/udt_util.cc
    ${ROCKSDB_SOURCE_DIR}/util/write_batch_util.cc
    ${ROCKSDB_SOURCE_DIR}/util/xxhash.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/agg_merge/agg_merge.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/backup/backup_engine.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/checkpoint/checkpoint_impl.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/compaction_filters.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/compaction_filters/remove_emptyvalue_compactionfilter.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/counted_fs.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/debug.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/env_mirror.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/env_timed.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/leveldb_options/leveldb_options.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/merge_operators.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/bytesxor.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/max.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/put.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/sortlist.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/string_append/stringappend.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/string_append/stringappend2.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/uint64add.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/object_registry.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/persistent_cache/block_cache_tier.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/persistent_cache/block_cache_tier_file.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/persistent_cache/block_cache_tier_metadata.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/persistent_cache/persistent_cache_tier.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/persistent_cache/volatile_tier_impl.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/simulator_cache/cache_simulator.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/simulator_cache/sim_cache.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/table_properties_collectors/compact_for_tiering_collector.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/table_properties_collectors/compact_on_deletion_collector.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/trace/file_trace_reader_writer.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/trace/replayer_impl.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/lock_manager.cc # Unused but dead code elimination is hard for some linkers
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/point/point_lock_tracker.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/point/point_lock_manager.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/range_tree_lock_manager.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/range_tree_lock_tracker.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/optimistic_transaction_db_impl.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/optimistic_transaction.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/pessimistic_transaction.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/pessimistic_transaction_db.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/snapshot_checker.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/transaction_base.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/transaction_db_mutex_impl.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/transaction_util.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/write_prepared_txn.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/write_prepared_txn_db.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/write_unprepared_txn.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/write_unprepared_txn_db.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/types_util.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/ttl/db_ttl_impl.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/wal_filter.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/write_batch_with_index/write_batch_with_index.cc # Unused but dead code elimination is hard for some linkers
    ${ROCKSDB_SOURCE_DIR}/utilities/write_batch_with_index/write_batch_with_index_internal.cc # Unused but dead code elimination is hard for some linkers
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/lib/locktree/concurrent_tree.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/lib/locktree/keyrange.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/lib/locktree/lock_request.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/lib/locktree/locktree.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/lib/locktree/manager.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/lib/locktree/range_buffer.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/lib/locktree/treenode.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/lib/locktree/txnid_set.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/lib/locktree/wfg.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/lib/standalone_port.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/lib/util/dbt.cc
    ${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/range/range_tree/lib/util/memarena.cc
    build_version.cc) # generated by hand (Mostly from contrib/rocksdb/TARGETS)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64")
  list(APPEND SOURCES
    "${ROCKSDB_SOURCE_DIR}/util/crc32c_ppc.c"
    "${ROCKSDB_SOURCE_DIR}/util/crc32c_ppc_asm.S")
endif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64")

if(HAS_ARMV8_CRC)
  list(APPEND SOURCES
    "${ROCKSDB_SOURCE_DIR}/util/crc32c_arm64.cc")
endif(HAS_ARMV8_CRC)

list(APPEND SOURCES
    ${ROCKSDB_SOURCE_DIR}/port/port_posix.cc
    ${ROCKSDB_SOURCE_DIR}/env/env_posix.cc
    ${ROCKSDB_SOURCE_DIR}/env/fs_posix.cc
    ${ROCKSDB_SOURCE_DIR}/env/io_posix.cc)

add_library(_rocksdb ${SOURCES})
add_library(ch_contrib::rocksdb ALIAS _rocksdb)
target_link_libraries(_rocksdb PRIVATE ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

# Not in the native build system but useful anyways:
# Make all functions in xxHash.h inline. Beneficial for performance: https://github.com/Cyan4973/xxHash/tree/v0.8.2#build-modifiers
target_compile_definitions (_rocksdb PRIVATE XXH_INLINE_ALL)

# SYSTEM is required to overcome some issues
target_include_directories(_rocksdb SYSTEM BEFORE INTERFACE "${ROCKSDB_SOURCE_DIR}/include")
