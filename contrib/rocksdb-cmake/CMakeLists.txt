## this file is extracted from `contrib/rocksdb/CMakeLists.txt`
set(ROCKSDB_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/rocksdb")
list(APPEND CMAKE_MODULE_PATH "${ROCKSDB_SOURCE_DIR}/cmake/modules/")

set(PORTABLE ON)
## always disable jemalloc for rocksdb by default
## because it introduces non-standard jemalloc APIs
option(WITH_JEMALLOC "build with JeMalloc" OFF)
option(WITH_SNAPPY "build with SNAPPY" ${USE_SNAPPY})
## lz4, zlib, zstd is enabled in ClickHouse by default
option(WITH_LZ4 "build with lz4" ON)
option(WITH_ZLIB "build with zlib" ON)
option(WITH_ZSTD "build with zstd" ON)

# third-party/folly is only validated to work on Linux and Windows for now.
# So only turn it on there by default.
if(CMAKE_SYSTEM_NAME MATCHES "Linux|Windows")
  if(MSVC AND MSVC_VERSION LESS 1910)
    # Folly does not compile with MSVC older than VS2017
    option(WITH_FOLLY_DISTRIBUTED_MUTEX "build with folly::DistributedMutex" OFF)
  else()
    option(WITH_FOLLY_DISTRIBUTED_MUTEX "build with folly::DistributedMutex" ON)
  endif()
else()
  option(WITH_FOLLY_DISTRIBUTED_MUTEX "build with folly::DistributedMutex" OFF)
endif()

if( NOT DEFINED CMAKE_CXX_STANDARD )
  set(CMAKE_CXX_STANDARD 11)
endif()

if(MSVC)
  option(WITH_XPRESS "build with windows built in compression" OFF)
  include("${ROCKSDB_SOURCE_DIR}/thirdparty.inc")
else()
  if(CMAKE_SYSTEM_NAME MATCHES "FreeBSD" AND NOT CMAKE_SYSTEM_NAME MATCHES "kFreeBSD")
    # FreeBSD has jemalloc as default malloc
    # but it does not have all the jemalloc files in include/...
    set(WITH_JEMALLOC ON)
  else()
    if(WITH_JEMALLOC)
      add_definitions(-DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE)
      list(APPEND THIRDPARTY_LIBS jemalloc)
    endif()
  endif()

  if(WITH_SNAPPY)
    add_definitions(-DSNAPPY)
    list(APPEND THIRDPARTY_LIBS snappy)
  endif()

  if(WITH_ZLIB)
    add_definitions(-DZLIB)
    list(APPEND THIRDPARTY_LIBS zlib)
  endif()

  if(WITH_LZ4)
    add_definitions(-DLZ4)
    list(APPEND THIRDPARTY_LIBS lz4)
  endif()

  if(WITH_ZSTD)
    add_definitions(-DZSTD)
    include_directories(${ZSTD_INCLUDE_DIR})
    include_directories("${ZSTD_INCLUDE_DIR}/common")
    include_directories("${ZSTD_INCLUDE_DIR}/dictBuilder")
    include_directories("${ZSTD_INCLUDE_DIR}/deprecated")

    list(APPEND THIRDPARTY_LIBS zstd)
  endif()
endif()

set(BUILD_VERSION_CC rocksdb_build_version.cc)
add_library(rocksdb_build_version OBJECT ${BUILD_VERSION_CC})

target_include_directories(rocksdb_build_version PRIVATE "${ROCKSDB_SOURCE_DIR}/util")

include(CheckCCompilerFlag)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64")
  CHECK_C_COMPILER_FLAG("-mcpu=power9" HAS_POWER9)
  if(HAS_POWER9)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mcpu=power9 -mtune=power9")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=power9 -mtune=power9")
  else()
    CHECK_C_COMPILER_FLAG("-mcpu=power8" HAS_POWER8)
    if(HAS_POWER8)
      set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mcpu=power8 -mtune=power8")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=power8 -mtune=power8")
    endif(HAS_POWER8)
  endif(HAS_POWER9)
  CHECK_C_COMPILER_FLAG("-maltivec" HAS_ALTIVEC)
  if(HAS_ALTIVEC)
    message(STATUS " HAS_ALTIVEC yes")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -maltivec")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -maltivec")
  endif(HAS_ALTIVEC)
endif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64")

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm64|ARM64")
  CHECK_C_COMPILER_FLAG("-march=armv8-a+crc+crypto" HAS_ARMV8_CRC)
  if(HAS_ARMV8_CRC)
    message(STATUS " HAS_ARMV8_CRC yes")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=armv8-a+crc+crypto -Wno-unused-function")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv8-a+crc+crypto -Wno-unused-function")
  endif(HAS_ARMV8_CRC)
endif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm64|ARM64")


include(CheckCXXSourceCompiles)
if(NOT MSVC)
  set(CMAKE_REQUIRED_FLAGS "-msse4.2 -mpclmul")
endif()

CHECK_CXX_SOURCE_COMPILES("
#include <cstdint>
#include <nmmintrin.h>
#include <wmmintrin.h>
int main() {
  volatile uint32_t x = _mm_crc32_u32(0, 0);
  const auto a = _mm_set_epi64x(0, 0);
  const auto b = _mm_set_epi64x(0, 0);
  const auto c = _mm_clmulepi64_si128(a, b, 0x00);
  auto d = _mm_cvtsi128_si64(c);
}
" HAVE_SSE42)
unset(CMAKE_REQUIRED_FLAGS)
if(HAVE_SSE42)
  add_definitions(-DHAVE_SSE42)
  add_definitions(-DHAVE_PCLMUL)
elseif(FORCE_SSE42)
  message(FATAL_ERROR "FORCE_SSE42=ON but unable to compile with SSE4.2 enabled")
endif()

CHECK_CXX_SOURCE_COMPILES("
#if defined(_MSC_VER) && !defined(__thread)
#define __thread __declspec(thread)
#endif
int main() {
  static __thread int tls;
}
" HAVE_THREAD_LOCAL)
if(HAVE_THREAD_LOCAL)
  add_definitions(-DROCKSDB_SUPPORT_THREAD_LOCAL)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  add_definitions(-DOS_MACOSX)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES arm)
    add_definitions(-DIOS_CROSS_COMPILE -DROCKSDB_LITE)
    # no debug info for IOS, that will make our library big
    add_definitions(-DNDEBUG)
  endif()
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
  add_definitions(-DOS_LINUX)
elseif(CMAKE_SYSTEM_NAME MATCHES "SunOS")
  add_definitions(-DOS_SOLARIS)
elseif(CMAKE_SYSTEM_NAME MATCHES "kFreeBSD")
  add_definitions(-DOS_GNU_KFREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  add_definitions(-DOS_FREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "NetBSD")
  add_definitions(-DOS_NETBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "OpenBSD")
  add_definitions(-DOS_OPENBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "DragonFly")
  add_definitions(-DOS_DRAGONFLYBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "Android")
  add_definitions(-DOS_ANDROID)
elseif(CMAKE_SYSTEM_NAME MATCHES "Windows")
  add_definitions(-DWIN32 -DOS_WIN -D_MBCS -DWIN64 -DNOMINMAX)
  if(MINGW)
    add_definitions(-D_WIN32_WINNT=_WIN32_WINNT_VISTA)
  endif()
endif()

if(NOT WIN32)
  add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)
endif()

option(WITH_FALLOCATE "build with fallocate" ON)
if(WITH_FALLOCATE)
  CHECK_CXX_SOURCE_COMPILES("
#include <fcntl.h>
#include <linux/falloc.h>
int main() {
 int fd = open(\"/dev/null\", 0);
 fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, 1024);
}
" HAVE_FALLOCATE)
  if(HAVE_FALLOCATE)
    add_definitions(-DROCKSDB_FALLOCATE_PRESENT)
  endif()
endif()

CHECK_CXX_SOURCE_COMPILES("
#include <fcntl.h>
int main() {
  int fd = open(\"/dev/null\", 0);
  sync_file_range(fd, 0, 1024, SYNC_FILE_RANGE_WRITE);
}
" HAVE_SYNC_FILE_RANGE_WRITE)
if(HAVE_SYNC_FILE_RANGE_WRITE)
  add_definitions(-DROCKSDB_RANGESYNC_PRESENT)
endif()

CHECK_CXX_SOURCE_COMPILES("
#include <pthread.h>
int main() {
  (void) PTHREAD_MUTEX_ADAPTIVE_NP;
}
" HAVE_PTHREAD_MUTEX_ADAPTIVE_NP)
if(HAVE_PTHREAD_MUTEX_ADAPTIVE_NP)
  add_definitions(-DROCKSDB_PTHREAD_ADAPTIVE_MUTEX)
endif()

include(CheckCXXSymbolExists)
if(CMAKE_SYSTEM_NAME MATCHES "^FreeBSD")
  check_cxx_symbol_exists(malloc_usable_size "${ROCKSDB_SOURCE_DIR}/malloc_np.h" HAVE_MALLOC_USABLE_SIZE)
else()
  check_cxx_symbol_exists(malloc_usable_size "${ROCKSDB_SOURCE_DIR}/malloc.h" HAVE_MALLOC_USABLE_SIZE)
endif()
if(HAVE_MALLOC_USABLE_SIZE)
  add_definitions(-DROCKSDB_MALLOC_USABLE_SIZE)
endif()

check_cxx_symbol_exists(sched_getcpu sched.h HAVE_SCHED_GETCPU)
if(HAVE_SCHED_GETCPU)
  add_definitions(-DROCKSDB_SCHED_GETCPU_PRESENT)
endif()

check_cxx_symbol_exists(getauxval auvx.h HAVE_AUXV_GETAUXVAL)
if(HAVE_AUXV_GETAUXVAL)
  add_definitions(-DROCKSDB_AUXV_GETAUXVAL_PRESENT)
endif()

include_directories(${ROCKSDB_SOURCE_DIR})
include_directories("${ROCKSDB_SOURCE_DIR}/include")
if(WITH_FOLLY_DISTRIBUTED_MUTEX)
  include_directories("${ROCKSDB_SOURCE_DIR}/third-party/folly")
endif()
find_package(Threads REQUIRED)

# Main library source code

set(SOURCES
        "${ROCKSDB_SOURCE_DIR}/cache/cache.cc"
        "${ROCKSDB_SOURCE_DIR}/cache/clock_cache.cc"
        "${ROCKSDB_SOURCE_DIR}/cache/lru_cache.cc"
        "${ROCKSDB_SOURCE_DIR}/cache/sharded_cache.cc"
        "${ROCKSDB_SOURCE_DIR}/db/arena_wrapped_db_iter.cc"
        "${ROCKSDB_SOURCE_DIR}/db/blob/blob_file_addition.cc"
        "${ROCKSDB_SOURCE_DIR}/db/blob/blob_file_builder.cc"
        "${ROCKSDB_SOURCE_DIR}/db/blob/blob_file_cache.cc"
        "${ROCKSDB_SOURCE_DIR}/db/blob/blob_file_garbage.cc"
        "${ROCKSDB_SOURCE_DIR}/db/blob/blob_file_meta.cc"
        "${ROCKSDB_SOURCE_DIR}/db/blob/blob_file_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/db/blob/blob_log_format.cc"
        "${ROCKSDB_SOURCE_DIR}/db/blob/blob_log_sequential_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/db/blob/blob_log_writer.cc"
        "${ROCKSDB_SOURCE_DIR}/db/builder.cc"
        "${ROCKSDB_SOURCE_DIR}/db/c.cc"
        "${ROCKSDB_SOURCE_DIR}/db/column_family.cc"
        "${ROCKSDB_SOURCE_DIR}/db/compacted_db_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/db/compaction/compaction.cc"
        "${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_iterator.cc"
        "${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_picker.cc"
        "${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_job.cc"
        "${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_picker_fifo.cc"
        "${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_picker_level.cc"
        "${ROCKSDB_SOURCE_DIR}/db/compaction/compaction_picker_universal.cc"
        "${ROCKSDB_SOURCE_DIR}/db/compaction/sst_partitioner.cc"
        "${ROCKSDB_SOURCE_DIR}/db/convenience.cc"
        "${ROCKSDB_SOURCE_DIR}/db/db_filesnapshot.cc"
        "${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_write.cc"
        "${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_compaction_flush.cc"
        "${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_files.cc"
        "${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_open.cc"
        "${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_debug.cc"
        "${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_experimental.cc"
        "${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_readonly.cc"
        "${ROCKSDB_SOURCE_DIR}/db/db_impl/db_impl_secondary.cc"
        "${ROCKSDB_SOURCE_DIR}/db/db_info_dumper.cc"
        "${ROCKSDB_SOURCE_DIR}/db/db_iter.cc"
        "${ROCKSDB_SOURCE_DIR}/db/dbformat.cc"
        "${ROCKSDB_SOURCE_DIR}/db/error_handler.cc"
        "${ROCKSDB_SOURCE_DIR}/db/event_helpers.cc"
        "${ROCKSDB_SOURCE_DIR}/db/experimental.cc"
        "${ROCKSDB_SOURCE_DIR}/db/external_sst_file_ingestion_job.cc"
        "${ROCKSDB_SOURCE_DIR}/db/file_indexer.cc"
        "${ROCKSDB_SOURCE_DIR}/db/flush_job.cc"
        "${ROCKSDB_SOURCE_DIR}/db/flush_scheduler.cc"
        "${ROCKSDB_SOURCE_DIR}/db/forward_iterator.cc"
        "${ROCKSDB_SOURCE_DIR}/db/import_column_family_job.cc"
        "${ROCKSDB_SOURCE_DIR}/db/internal_stats.cc"
        "${ROCKSDB_SOURCE_DIR}/db/logs_with_prep_tracker.cc"
        "${ROCKSDB_SOURCE_DIR}/db/log_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/db/log_writer.cc"
        "${ROCKSDB_SOURCE_DIR}/db/malloc_stats.cc"
        "${ROCKSDB_SOURCE_DIR}/db/memtable.cc"
        "${ROCKSDB_SOURCE_DIR}/db/memtable_list.cc"
        "${ROCKSDB_SOURCE_DIR}/db/merge_helper.cc"
        "${ROCKSDB_SOURCE_DIR}/db/merge_operator.cc"
        "${ROCKSDB_SOURCE_DIR}/db/output_validator.cc"
        "${ROCKSDB_SOURCE_DIR}/db/periodic_work_scheduler.cc"
        "${ROCKSDB_SOURCE_DIR}/db/range_del_aggregator.cc"
        "${ROCKSDB_SOURCE_DIR}/db/range_tombstone_fragmenter.cc"
        "${ROCKSDB_SOURCE_DIR}/db/repair.cc"
        "${ROCKSDB_SOURCE_DIR}/db/snapshot_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/db/table_cache.cc"
        "${ROCKSDB_SOURCE_DIR}/db/table_properties_collector.cc"
        "${ROCKSDB_SOURCE_DIR}/db/transaction_log_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/db/trim_history_scheduler.cc"
        "${ROCKSDB_SOURCE_DIR}/db/version_builder.cc"
        "${ROCKSDB_SOURCE_DIR}/db/version_edit.cc"
        "${ROCKSDB_SOURCE_DIR}/db/version_edit_handler.cc"
        "${ROCKSDB_SOURCE_DIR}/db/version_set.cc"
        "${ROCKSDB_SOURCE_DIR}/db/wal_edit.cc"
        "${ROCKSDB_SOURCE_DIR}/db/wal_manager.cc"
        "${ROCKSDB_SOURCE_DIR}/db/write_batch.cc"
        "${ROCKSDB_SOURCE_DIR}/db/write_batch_base.cc"
        "${ROCKSDB_SOURCE_DIR}/db/write_controller.cc"
        "${ROCKSDB_SOURCE_DIR}/db/write_thread.cc"
        "${ROCKSDB_SOURCE_DIR}/env/env.cc"
        "${ROCKSDB_SOURCE_DIR}/env/env_chroot.cc"
        "${ROCKSDB_SOURCE_DIR}/env/env_encryption.cc"
        "${ROCKSDB_SOURCE_DIR}/env/env_hdfs.cc"
        "${ROCKSDB_SOURCE_DIR}/env/file_system.cc"
        "${ROCKSDB_SOURCE_DIR}/env/file_system_tracer.cc"
        "${ROCKSDB_SOURCE_DIR}/env/mock_env.cc"
        "${ROCKSDB_SOURCE_DIR}/file/delete_scheduler.cc"
        "${ROCKSDB_SOURCE_DIR}/file/file_prefetch_buffer.cc"
        "${ROCKSDB_SOURCE_DIR}/file/file_util.cc"
        "${ROCKSDB_SOURCE_DIR}/file/filename.cc"
        "${ROCKSDB_SOURCE_DIR}/file/random_access_file_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/file/read_write_util.cc"
        "${ROCKSDB_SOURCE_DIR}/file/readahead_raf.cc"
        "${ROCKSDB_SOURCE_DIR}/file/sequence_file_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/file/sst_file_manager_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/file/writable_file_writer.cc"
        "${ROCKSDB_SOURCE_DIR}/logging/auto_roll_logger.cc"
        "${ROCKSDB_SOURCE_DIR}/logging/event_logger.cc"
        "${ROCKSDB_SOURCE_DIR}/logging/log_buffer.cc"
        "${ROCKSDB_SOURCE_DIR}/memory/arena.cc"
        "${ROCKSDB_SOURCE_DIR}/memory/concurrent_arena.cc"
        "${ROCKSDB_SOURCE_DIR}/memory/jemalloc_nodump_allocator.cc"
        "${ROCKSDB_SOURCE_DIR}/memory/memkind_kmem_allocator.cc"
        "${ROCKSDB_SOURCE_DIR}/memtable/alloc_tracker.cc"
        "${ROCKSDB_SOURCE_DIR}/memtable/hash_linklist_rep.cc"
        "${ROCKSDB_SOURCE_DIR}/memtable/hash_skiplist_rep.cc"
        "${ROCKSDB_SOURCE_DIR}/memtable/skiplistrep.cc"
        "${ROCKSDB_SOURCE_DIR}/memtable/vectorrep.cc"
        "${ROCKSDB_SOURCE_DIR}/memtable/write_buffer_manager.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/histogram.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/histogram_windowing.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/in_memory_stats_history.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/instrumented_mutex.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/iostats_context.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/perf_context.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/perf_level.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/persistent_stats_history.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/statistics.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/thread_status_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/thread_status_updater.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/thread_status_util.cc"
        "${ROCKSDB_SOURCE_DIR}/monitoring/thread_status_util_debug.cc"
        "${ROCKSDB_SOURCE_DIR}/options/cf_options.cc"
        "${ROCKSDB_SOURCE_DIR}/options/configurable.cc"
        "${ROCKSDB_SOURCE_DIR}/options/customizable.cc"
        "${ROCKSDB_SOURCE_DIR}/options/db_options.cc"
        "${ROCKSDB_SOURCE_DIR}/options/options.cc"
        "${ROCKSDB_SOURCE_DIR}/options/options_helper.cc"
        "${ROCKSDB_SOURCE_DIR}/options/options_parser.cc"
        "${ROCKSDB_SOURCE_DIR}/port/stack_trace.cc"
        "${ROCKSDB_SOURCE_DIR}/table/adaptive/adaptive_table_factory.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/binary_search_index_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/block.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/block_based_filter_block.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/block_based_table_builder.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/block_based_table_factory.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/block_based_table_iterator.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/block_based_table_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/block_builder.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/block_prefetcher.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/block_prefix_index.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/data_block_hash_index.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/data_block_footer.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/filter_block_reader_common.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/filter_policy.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/flush_block_policy.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/full_filter_block.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/hash_index_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/index_builder.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/index_reader_common.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/parsed_full_filter_block.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/partitioned_filter_block.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/partitioned_index_iterator.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/partitioned_index_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/reader_common.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_based/uncompression_dict_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/table/block_fetcher.cc"
        "${ROCKSDB_SOURCE_DIR}/table/cuckoo/cuckoo_table_builder.cc"
        "${ROCKSDB_SOURCE_DIR}/table/cuckoo/cuckoo_table_factory.cc"
        "${ROCKSDB_SOURCE_DIR}/table/cuckoo/cuckoo_table_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/table/format.cc"
        "${ROCKSDB_SOURCE_DIR}/table/get_context.cc"
        "${ROCKSDB_SOURCE_DIR}/table/iterator.cc"
        "${ROCKSDB_SOURCE_DIR}/table/merging_iterator.cc"
        "${ROCKSDB_SOURCE_DIR}/table/meta_blocks.cc"
        "${ROCKSDB_SOURCE_DIR}/table/persistent_cache_helper.cc"
        "${ROCKSDB_SOURCE_DIR}/table/plain/plain_table_bloom.cc"
        "${ROCKSDB_SOURCE_DIR}/table/plain/plain_table_builder.cc"
        "${ROCKSDB_SOURCE_DIR}/table/plain/plain_table_factory.cc"
        "${ROCKSDB_SOURCE_DIR}/table/plain/plain_table_index.cc"
        "${ROCKSDB_SOURCE_DIR}/table/plain/plain_table_key_coding.cc"
        "${ROCKSDB_SOURCE_DIR}/table/plain/plain_table_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/table/sst_file_dumper.cc"
        "${ROCKSDB_SOURCE_DIR}/table/sst_file_reader.cc"
        "${ROCKSDB_SOURCE_DIR}/table/sst_file_writer.cc"
        "${ROCKSDB_SOURCE_DIR}/table/table_factory.cc"
        "${ROCKSDB_SOURCE_DIR}/table/table_properties.cc"
        "${ROCKSDB_SOURCE_DIR}/table/two_level_iterator.cc"
        "${ROCKSDB_SOURCE_DIR}/test_util/sync_point.cc"
        "${ROCKSDB_SOURCE_DIR}/test_util/sync_point_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/test_util/testutil.cc"
        "${ROCKSDB_SOURCE_DIR}/test_util/transaction_test_util.cc"
        "${ROCKSDB_SOURCE_DIR}/tools/block_cache_analyzer/block_cache_trace_analyzer.cc"
        "${ROCKSDB_SOURCE_DIR}/tools/dump/db_dump_tool.cc"
        "${ROCKSDB_SOURCE_DIR}/tools/io_tracer_parser_tool.cc"
        "${ROCKSDB_SOURCE_DIR}/tools/ldb_cmd.cc"
        "${ROCKSDB_SOURCE_DIR}/tools/ldb_tool.cc"
        "${ROCKSDB_SOURCE_DIR}/tools/sst_dump_tool.cc"
        "${ROCKSDB_SOURCE_DIR}/tools/trace_analyzer_tool.cc"
        "${ROCKSDB_SOURCE_DIR}/trace_replay/trace_replay.cc"
        "${ROCKSDB_SOURCE_DIR}/trace_replay/block_cache_tracer.cc"
        "${ROCKSDB_SOURCE_DIR}/trace_replay/io_tracer.cc"
        "${ROCKSDB_SOURCE_DIR}/util/coding.cc"
        "${ROCKSDB_SOURCE_DIR}/util/compaction_job_stats_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/util/comparator.cc"
        "${ROCKSDB_SOURCE_DIR}/util/compression_context_cache.cc"
        "${ROCKSDB_SOURCE_DIR}/util/concurrent_task_limiter_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/util/crc32c.cc"
        "${ROCKSDB_SOURCE_DIR}/util/dynamic_bloom.cc"
        "${ROCKSDB_SOURCE_DIR}/util/hash.cc"
        "${ROCKSDB_SOURCE_DIR}/util/murmurhash.cc"
        "${ROCKSDB_SOURCE_DIR}/util/random.cc"
        "${ROCKSDB_SOURCE_DIR}/util/rate_limiter.cc"
        "${ROCKSDB_SOURCE_DIR}/util/slice.cc"
        "${ROCKSDB_SOURCE_DIR}/util/file_checksum_helper.cc"
        "${ROCKSDB_SOURCE_DIR}/util/status.cc"
        "${ROCKSDB_SOURCE_DIR}/util/string_util.cc"
        "${ROCKSDB_SOURCE_DIR}/util/thread_local.cc"
        "${ROCKSDB_SOURCE_DIR}/util/threadpool_imp.cc"
        "${ROCKSDB_SOURCE_DIR}/util/xxhash.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/backupable/backupable_db.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/blob_db/blob_compaction_filter.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/blob_db/blob_db.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/blob_db/blob_db_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/blob_db/blob_db_impl_filesnapshot.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/blob_db/blob_dump_tool.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/blob_db/blob_file.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/cassandra/cassandra_compaction_filter.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/cassandra/format.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/cassandra/merge_operator.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/checkpoint/checkpoint_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/compaction_filters/remove_emptyvalue_compactionfilter.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/debug.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/env_mirror.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/env_timed.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/fault_injection_env.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/fault_injection_fs.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/leveldb_options/leveldb_options.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/memory/memory_util.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/bytesxor.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/max.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/put.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/sortlist.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/string_append/stringappend.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/string_append/stringappend2.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/merge_operators/uint64add.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/object_registry.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/option_change_migration/option_change_migration.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/options/options_util.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/persistent_cache/block_cache_tier.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/persistent_cache/block_cache_tier_file.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/persistent_cache/block_cache_tier_metadata.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/persistent_cache/persistent_cache_tier.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/persistent_cache/volatile_tier_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/simulator_cache/cache_simulator.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/simulator_cache/sim_cache.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/table_properties_collectors/compact_on_deletion_collector.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/trace/file_trace_reader_writer.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/lock_manager.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/point/point_lock_tracker.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/lock/point/point_lock_manager.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/optimistic_transaction_db_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/optimistic_transaction.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/pessimistic_transaction.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/pessimistic_transaction_db.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/snapshot_checker.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/transaction_base.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/transaction_db_mutex_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/transaction_util.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/write_prepared_txn.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/write_prepared_txn_db.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/write_unprepared_txn.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/transactions/write_unprepared_txn_db.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/ttl/db_ttl_impl.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/write_batch_with_index/write_batch_with_index.cc"
        "${ROCKSDB_SOURCE_DIR}/utilities/write_batch_with_index/write_batch_with_index_internal.cc"
        $<TARGET_OBJECTS:rocksdb_build_version>)

if(HAVE_SSE42 AND NOT MSVC)
  set_source_files_properties(
    "${ROCKSDB_SOURCE_DIR}/util/crc32c.cc"
    PROPERTIES COMPILE_FLAGS "-msse4.2 -mpclmul")
endif()

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
    "${ROCKSDB_SOURCE_DIR}/port/port_posix.cc"
    "${ROCKSDB_SOURCE_DIR}/env/env_posix.cc"
    "${ROCKSDB_SOURCE_DIR}/env/fs_posix.cc"
    "${ROCKSDB_SOURCE_DIR}/env/io_posix.cc")

if(WITH_FOLLY_DISTRIBUTED_MUTEX)
  list(APPEND SOURCES
    "${ROCKSDB_SOURCE_DIR}/third-party/folly/folly/detail/Futex.cpp"
    "${ROCKSDB_SOURCE_DIR}/third-party/folly/folly/synchronization/AtomicNotification.cpp"
    "${ROCKSDB_SOURCE_DIR}/third-party/folly/folly/synchronization/DistributedMutex.cpp"
    "${ROCKSDB_SOURCE_DIR}/third-party/folly/folly/synchronization/ParkingLot.cpp"
    "${ROCKSDB_SOURCE_DIR}/third-party/folly/folly/synchronization/WaitOptions.cpp")
endif()

set(ROCKSDB_STATIC_LIB rocksdb)

add_library(${ROCKSDB_STATIC_LIB} STATIC ${SOURCES})
target_link_libraries(${ROCKSDB_STATIC_LIB} PRIVATE
  ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
