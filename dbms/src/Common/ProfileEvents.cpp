#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnArray.h>

/// Available events. Add something here as you wish.
#define APPLY_FOR_EVENTS(M) \
    M(Query, \
      "queries_total", \
      "Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.") \
    M(SelectQuery, \
      "select_queries_total", \
      "Same as Query, but only for SELECT queries.") \
    M(InsertQuery, \
      "insert_queries_total", \
      "Same as Query, but only for INSERT queries.") \
    M(FileOpen, \
      "files_open_total", \
      "Number of files opened.") \
    M(Seek, \
      "seek_total", \
      "Number of times the 'lseek' function was called.") \
    M(ReadBufferFromFileDescriptorRead, \
      "read_buffer_from_file_descriptor_reads_total", \
      "Number of reads (read/pread) from a file descriptor. Does not include sockets.") \
    M(ReadBufferFromFileDescriptorReadFailed, \
      "read_buffer_from_file_descriptor_reads_failed_total", \
      "Number of times the read (read/pread) from a file descriptor have failed.") \
    M(ReadBufferFromFileDescriptorReadBytes, \
      "read_buffer_from_file_descriptor_read_bytes", \
      "Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.") \
    M(WriteBufferFromFileDescriptorWrite, \
      "write_buffer_from_file_descriptor_writes_total", \
      "Number of writes (write/pwrite) to a file descriptor. Does not include sockets.") \
    M(WriteBufferFromFileDescriptorWriteFailed, \
      "write_buffer_from_file_descriptor_writes_failed_total", \
      "Number of times the write (write/pwrite) to a file descriptor have failed.") \
    M(WriteBufferFromFileDescriptorWriteBytes, \
      "write_buffer_from_file_descriptor_write_bytes", \
      "Number of bytes written to file descriptors. If the file is compressed, this will show compressed data size.") \
    M(ReadBufferAIORead, \
      "read_buffer_aio_read_total", \
      "") \
    M(ReadBufferAIOReadBytes, \
      "read_buffer_aio_read_bytes", \
      "") \
    M(WriteBufferAIOWrite, \
      "write_buffer_aio_write_total", \
      "") \
    M(WriteBufferAIOWriteBytes, \
      "write_buffer_aio_write_bytes", \
      "") \
    M(ReadCompressedBytes, \
      "read_compressed_bytes", \
      "") \
    M(CompressedReadBufferBlocks, \
      "compressed_read_buffer_blocks", \
      "") \
    M(CompressedReadBufferBytes, \
      "compressed_read_buffer_bytes", \
      "") \
    M(UncompressedCacheHits, \
      "uncompressed_cache_hits_total", \
      "") \
    M(UncompressedCacheMisses, \
      "uncompressed_cache_misses_total", \
      "") \
    M(UncompressedCacheWeightLost, \
      "uncompressed_cache_weight_lost", \
      "") \
    M(IOBufferAllocs, \
      "io_buffer_allocs_total", \
      "") \
    M(IOBufferAllocBytes, \
      "io_buffer_alloc_bytes", \
      "") \
    M(ArenaAllocChunks, \
      "arena_alloc_chunks", \
      "") \
    M(ArenaAllocBytes, \
      "arena_alloc_bytes", \
      "") \
    M(FunctionExecute, \
      "function_execute", \
      "") \
    M(TableFunctionExecute, \
      "table_function_execute", \
      "") \
    M(MarkCacheHits, \
      "mark_cache_hits_total", \
      "") \
    M(MarkCacheMisses, \
      "mark_cache_misses_total", \
      "") \
    M(CreatedReadBufferOrdinary, \
      "created_read_buffer_ordinary", \
      "") \
    M(CreatedReadBufferAIO, \
      "created_read_buffer_aio_total", \
      "") \
    M(CreatedReadBufferAIOFailed, \
      "created_read_buffer_aio_failed_total", \
      "") \
    M(CreatedWriteBufferOrdinary, \
      "created_write_buffer_ordinary_total", \
      "") \
    M(CreatedWriteBufferAIO, \
      "created_write_buffer_aio_total", \
      "") \
    M(CreatedWriteBufferAIOFailed, \
      "created_write_buffer_aio_failed_total", \
      "") \
    M(DiskReadElapsedMicroseconds, \
      "disk_read_elapsed_microseconds", \
      "Total time spent waiting for read syscall. This include reads from page cache.") \
    M(DiskWriteElapsedMicroseconds, \
      "disk_write_elapsed_microseconds", \
      "Total time spent waiting for write syscall. This include writes to page cache.") \
    M(NetworkReceiveElapsedMicroseconds, \
      "network_receive_elapsed_microseconds", \
      "") \
    M(NetworkSendElapsedMicroseconds, \
      "network_send_elapsed_microseconds", \
      "") \
    M(ThrottlerSleepMicroseconds, \
      "throttler_sleep_microseconds", \
      "Total time a queries_total was sleeping to conform the 'max_network_bandwidth' setting.") \
    \
    M(QueryMaskingRulesMatch, \
      "query_masking_rules_match_total", \
      "Number of times query masking rules was successfully matched.") \
    \
    M(ReplicatedPartFetches, \
      "replicated_part_fetches_total", \
      "Number of times a data part was downloaded from replica of a ReplicatedMergeTree table.") \
    M(ReplicatedPartFailedFetches, \
      "replicated_part_failed_fetches_total", \
      "") \
    M(ObsoleteReplicatedParts, \
      "obsolete_replicated_parts_total", \
      "") \
    M(ReplicatedPartMerges, \
      "replicated_part_merges_total", \
      "") \
    M(ReplicatedPartFetchesOfMerged, \
      "replicated_part_fetches_of_merged_total", \
      "Number of times we prefer to download already merged part from replica of ReplicatedMergeTree table instead of performing a merge ourself (usually we prefer doing a merge ourself to save network traffic). This happens when we have not all source parts to perform a merge or when the data part is old enough.") \
    M(ReplicatedPartMutations, \
      "replicated_part_mutations_total", \
      "") \
    M(ReplicatedPartChecks, \
      "replicated_part_checks_total", \
      "") \
    M(ReplicatedPartChecksFailed, \
      "replicated_part_checks_failed_total", \
      "") \
    M(ReplicatedDataLoss, \
      "replicated_data_loss_total", \
      "Number of times a data part that we wanted doesn't exist on any replica (even on replicas that are offline right now). That data parts are definitely lost. This is normal due to asynchronous replication (if quorum inserts were not enabled), when the replica on which the data part was written was failed and when it became online after fail it doesn't contain that data part.") \
    \
    M(InsertedRows, \
      "inserted_rows_total", \
      "Number of rows INSERTed to all tables.") \
    M(InsertedBytes, \
      "inserted_bytes", \
      "Number of bytes (uncompressed; for columns as they stored in memory) INSERTed to all tables.") \
    M(DelayedInserts, \
      "delayed_inserts_total", \
      "Number of times the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.") \
    M(RejectedInserts, \
      "rejected_inserts_total", \
      "Number of times the INSERT of a block to a MergeTree table was rejected with 'Too many parts' exception due to high number of active data parts for partition.") \
    M(DelayedInsertsMilliseconds, \
      "delayed_inserts_milliseconds", \
      "Total number of milliseconds spent while the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.") \
    M(DuplicatedInsertedBlocks, \
      "duplicated_inserted_blocks", \
      "Number of times the INSERTed block to a ReplicatedMergeTree table was deduplicated.") \
    \
    M(ZooKeeperInit, \
      "zookeeper_init", \
      "") \
    M(ZooKeeperTransactions, \
      "zookeeper_transactions", \
      "") \
    M(ZooKeeperList, \
      "zookeeper_list", \
      "") \
    M(ZooKeeperCreate, \
      "zookeeper_create", \
      "") \
    M(ZooKeeperRemove, \
      "zookeeper_remove", \
      "") \
    M(ZooKeeperExists, \
      "zookeeper_exists", \
      "") \
    M(ZooKeeperGet, \
      "zookeeper_get", \
      "") \
    M(ZooKeeperSet, \
      "zookeeper_set", \
      "") \
    M(ZooKeeperMulti, \
      "zookeeper_multi", \
      "") \
    M(ZooKeeperCheck, \
      "zookeeper_check", \
      "") \
    M(ZooKeeperClose, \
      "zookeeper_close", \
      "") \
    M(ZooKeeperWatchResponse, \
      "zookeeper_watch_response", \
      "") \
    M(ZooKeeperUserExceptions, \
      "zookeeper_user_exceptions", \
      "") \
    M(ZooKeeperHardwareExceptions, \
      "zookeeper_hardware_exceptions", \
      "") \
    M(ZooKeeperOtherExceptions, \
      "zookeeper_other_exceptions", \
      "") \
    M(ZooKeeperWaitMicroseconds, \
      "zookeeper_wait_microseconds", \
      "") \
    M(ZooKeeperBytesSent, \
      "zookeeper_bytes_sent", \
      "") \
    M(ZooKeeperBytesReceived, \
      "zookeeper_bytes_received", \
      "") \
    \
    M(DistributedConnectionFailTry, \
      "distributed_connection_fail_try", \
      "") \
    M(DistributedConnectionMissingTable, \
      "distributed_connection_missing_table", \
      "") \
    M(DistributedConnectionStaleReplica, \
      "distributed_connection_stale_replica", \
      "") \
    M(DistributedConnectionFailAtAll, \
      "distributed_connection_fail_at_all", \
      "") \
    \
    M(CompileAttempt, \
      "compile_attempt_total", \
      "Number of times a compilation of generated C++ code was initiated.") \
    M(CompileSuccess, \
      "compile_success_total", \
      "Number of times a compilation of generated C++ code was successful.") \
    \
    M(CompileFunction, \
      "compile_function_total", \
      "Number of times a compilation of generated LLVM code (to create fused function for complex expressions) was initiated.") \
    M(CompiledFunctionExecute, \
      "compiled_function_execute_total", \
      "Number of times a compiled function was executed.") \
    M(CompileExpressionsMicroseconds, \
      "compile_expressions_microseconds", \
      "Total time spent for compilation of expressions to LLVM code.") \
    M(CompileExpressionsBytes, \
      "compile_expressions_bytes", \
      "Number of bytes used for expressions compilation.") \
    \
    M(ExternalSortWritePart, \
      "external_sort_write_part", \
      "") \
    M(ExternalSortMerge, \
      "external_sort_merge", \
      "") \
    M(ExternalAggregationWritePart, \
      "external_aggregation_write_part", \
      "") \
    M(ExternalAggregationMerge, \
      "external_aggregation_merge", \
      "") \
    M(ExternalAggregationCompressedBytes, \
      "external_aggregation_compressed_bytes", \
      "") \
    M(ExternalAggregationUncompressedBytes, \
      "external_aggregation_uncompressed_bytes", \
      "") \
    \
    M(SlowRead, \
      "slow_reads_total", \
      "Number of reads from a file that were slow. This indicate system overload. Thresholds are controlled by read_backoff_* settings.") \
    M(ReadBackoff, \
      "read_backoff_total", \
      "Number of times the number of query processing threads was lowered due to slow reads.") \
    \
    M(ReplicaYieldLeadership, \
      "replica_yield_leadership_total", \
      "Number of times Replicated table was yielded its leadership due to large replication lag relative to other replicas.") \
    M(ReplicaPartialShutdown, \
      "replica_partial_shutdown", \
      "") \
    \
    M(SelectedParts, \
      "selected_parts_total", \
      "Number of data parts selected to read from a MergeTree table.") \
    M(SelectedRanges, \
      "selected_ranges_total", \
      "Number of (non-adjacent) ranges in all data parts selected to read from a MergeTree table.") \
    M(SelectedMarks, \
      "selected_marks_total", \
      "Number of marks (index granules) selected to read from a MergeTree table.") \
    \
    M(Merge, \
      "merges_total", \
      "Number of launched background merges.") \
    M(MergedRows, \
      "merged_rows_total", \
      "Rows read for background merges. This is the number of rows before merge.") \
    M(MergedUncompressedBytes, \
      "merged_uncompressed_bytes", \
      "Uncompressed bytes (for columns as they stored in memory) that was read for background merges. This is the number before merge.") \
    M(MergesTimeMilliseconds, \
      "merges_time_milliseconds", \
      "Total time spent for background merges.")\
    \
    M(MergeTreeDataWriterRows, \
      "merge_tree_data_writer_rows_total", \
      "Number of rows INSERTed to MergeTree tables.") \
    M(MergeTreeDataWriterUncompressedBytes, \
      "merge_tree_data_writer_uncompressed_bytes", \
      "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables.") \
    M(MergeTreeDataWriterCompressedBytes, \
      "merge_tree_data_writer_compressed_bytes", \
      "Bytes written to filesystem for data INSERTed to MergeTree tables.") \
    M(MergeTreeDataWriterBlocks, \
      "merge_tree_data_writer_blocks", \
      "Number of blocks INSERTed to MergeTree tables. Each block forms a data part of level zero.") \
    M(MergeTreeDataWriterBlocksAlreadySorted, \
      "merge_tree_data_writer_blocks_already_sorted_total", \
      "Number of blocks INSERTed to MergeTree tables that appeared to be already sorted.") \
    \
    M(CannotRemoveEphemeralNode, \
      "cannot_remove_ephemeral_node_total", \
      "Number of times an error happened while trying to remove ephemeral node. This is not an issue, because our implementation of ZooKeeper library guarantee that the session will expire and the node will be removed.") \
    M(LeaderElectionAcquiredLeadership, \
      "leader_election_acquired_leadership_total", \
      "Number of times a ReplicatedMergeTree table became a leader. Leader replica is responsible for assigning merges, cleaning old blocks for deduplications and a few more bookkeeping tasks.") \
    \
    M(RegexpCreated, \
      "regexp_created_total", \
      "Compiled regular expressions. Identical regular expressions compiled just once and cached forever.") \
    M(ContextLock, \
      "context_lock", \
      "Number of times the lock of Context was acquired or tried to acquire. This is global lock.") \
    \
    M(StorageBufferFlush, \
      "storage_buffer_flush", \
      "") \
    M(StorageBufferErrorOnFlush, \
      "storage_buffer_error_on_flush", \
      "") \
    M(StorageBufferPassedAllMinThresholds, \
      "storage_buffer_passed_all_min_thresholds", \
      "") \
    M(StorageBufferPassedTimeMaxThreshold, \
      "storage_buffer_passed_time_max_threshold", \
      "") \
    M(StorageBufferPassedRowsMaxThreshold, \
      "storage_buffer_passed_rows_max_threshold", \
      "") \
    M(StorageBufferPassedBytesMaxThreshold, \
      "storage_buffer_passed_bytes_max_threshold", \
      "") \
    \
    M(DictCacheKeysRequested, \
      "dict_cache_keys_requested", \
      "") \
    M(DictCacheKeysRequestedMiss, \
      "dict_cache_keys_requested_miss", \
      "") \
    M(DictCacheKeysRequestedFound, \
      "dict_cache_keys_requested_found", \
      "") \
    M(DictCacheKeysExpired, \
      "dict_cache_keys_expired", \
      "") \
    M(DictCacheKeysNotFound, \
      "dict_cache_keys_not_found", \
      "") \
    M(DictCacheKeysHit, \
      "dict_cache_keys_hit", \
      "") \
    M(DictCacheRequestTimeNs, \
      "dict_cache_request_time_ns", \
      "") \
    M(DictCacheRequests, \
      "dict_cache_requests", \
      "") \
    M(DictCacheLockWriteNs, \
      "dict_cache_lock_write_ns", \
      "") \
    M(DictCacheLockReadNs, \
      "dict_cache_lock_read_ns", \
      "") \
    \
    M(DistributedSyncInsertionTimeoutExceeded, \
      "distributed_sync_insertion_timeout_exceeded", \
      "") \
    M(DataAfterMergeDiffersFromReplica, \
      "data_after_merge_differs_from_replica", \
      "") \
    M(DataAfterMutationDiffersFromReplica, \
      "data_after_mutation_differs_from_replica", \
      "") \
    M(PolygonsAddedToPool, \
      "polygons_added_to_pool", \
      "") \
    M(PolygonsInPoolAllocatedBytes, \
      "polygons_in_pool_allocated_bytes", \
      "") \
    M(RWLockAcquiredReadLocks, \
      "rw_lock_acquired_read_locks", \
      "") \
    M(RWLockAcquiredWriteLocks, \
      "rw_lock_acquired_write_locks", \
      "") \
    M(RWLockReadersWaitMilliseconds, \
      "rw_lock_readers_wait_milliseconds", \
      "") \
    M(RWLockWritersWaitMilliseconds, \
      "rw_lock_writers_wait_milliseconds", \
      "") \
    M(NetworkErrors, \
      "network_errors", \
      "") \
    \
    M(RealTimeMicroseconds, \
      "real_time_microseconds", \
      "Total (wall clock) time spent in processing (queries and other tasks) threads (not that this is a sum).") \
    M(UserTimeMicroseconds, \
      "user_time_microseconds", \
      "Total time spent in processing (queries and other tasks) threads executing CPU instructions in user space. This include time CPU pipeline was stalled due to cache misses, branch mispredictions, hyper-threading, etc.") \
    M(SystemTimeMicroseconds, \
      "system_time_microseconds", \
      "Total time spent in processing (queries and other tasks) threads executing CPU instructions in OS kernel space. This include time CPU pipeline was stalled due to cache misses, branch mispredictions, hyper-threading, etc.") \
    M(SoftPageFaults, \
      "soft_page_faults", \
      "") \
    M(HardPageFaults, \
      "hard_page_faults", \
      "") \
    M(VoluntaryContextSwitches, \
      "voluntary_context_switches", \
      "") \
    M(InvoluntaryContextSwitches, \
      "involuntary_context_switches", \
      "") \
    \
    M(OSIOWaitMicroseconds, \
      "osio_wait_microseconds", \
      "Total time a thread spent waiting for a result of IO operation, from the OS point of view. This is real IO that doesn't include page cache.") \
    M(OSCPUWaitMicroseconds, \
      "oscpu_wait_microseconds", \
      "Total time a thread was ready for execution but waiting to be scheduled by OS, from the OS point of view.") \
    M(OSCPUVirtualTimeMicroseconds, \
      "oscpu_virtual_time_microseconds", \
      "CPU time spent seen by OS. Does not include involuntary waits due to virtualization.") \
    M(OSReadBytes, \
      "os_read_bytes", \
      "Number of bytes read from disks or block devices. Doesn't include bytes read from page cache. May include excessive data due to block size, readahead, etc.") \
    M(OSWriteBytes, \
      "os_write_bytes", \
      "Number of bytes written to disks or block devices. Doesn't include bytes that are in page cache dirty pages. May not include data that was written by OS asynchronously.") \
    M(OSReadChars, \
      "os_read_chars", \
      "Number of bytes read from filesystem, including page cache.") \
    M(OSWriteChars, \
      "os_write_chars", \
      "Number of bytes written to filesystem, including page cache.") \
    M(CreatedHTTPConnections, \
      "created_http_connections", \
      "Total amount of created HTTP connections (closed or opened).") \
    \
    M(CannotWriteToWriteBufferDiscard, \
      "cannot_write_to_write_buffer_discard_total", \
      "Number of stack traces dropped by query profiler or signal handler because pipe is full or cannot write to pipe.") \
    M(QueryProfilerSignalOverruns, \
      "query_profiler_signal_overruns_total", \
      "Number of times we drop processing of a signal due to overrun plus the number of signals that OS has not delivered due to overrun.") \

namespace ProfileEvents
{

#define M(NAME, SNAKE_NAME, DOCUMENTATION) extern const Event NAME = __COUNTER__;
    APPLY_FOR_EVENTS(M)
#undef M
constexpr Event END = __COUNTER__;

/// Global variable, initialized by zeros.
Counter global_counters_array[END] {};
/// Initialize global counters statically
Counters global_counters(global_counters_array);

const Event Counters::num_counters = END;


Counters::Counters(VariableContext level_, Counters * parent_)
    : counters_holder(new Counter[num_counters] {}),
      parent(parent_),
      level(level_)
{
    counters = counters_holder.get();
}

void Counters::resetCounters()
{
    if (counters)
    {
        for (Event i = 0; i < num_counters; ++i)
            counters[i].store(0, std::memory_order_relaxed);
    }
}

void Counters::reset()
{
    parent = nullptr;
    resetCounters();
}

Counters Counters::getPartiallyAtomicSnapshot() const
{
    Counters res(VariableContext::Snapshot, nullptr);
    for (Event i = 0; i < num_counters; ++i)
        res.counters[i].store(counters[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
    return res;
}

const char * getName(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, SNAKE_NAME, DOCUMENTATION) #NAME,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

const char * getNameSnake(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, SNAKE_NAME, DOCUMENTATION) SNAKE_NAME,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

const char * getDocumentation(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, SNAKE_NAME, DOCUMENTATION) DOCUMENTATION,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}


Event end() { return END; }


void increment(Event event, Count amount)
{
    DB::CurrentThread::getProfileEvents().increment(event, amount);
}

}

#undef APPLY_FOR_EVENTS
