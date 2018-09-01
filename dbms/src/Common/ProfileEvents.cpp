#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnArray.h>

/// Available events. Add something here as you wish.
#define APPLY_FOR_EVENTS(M) \
    M(Query, "Number of queries started to be interpreted and maybe executed. Does not include queries that are failed to parse, that are rejected due to AST size limits; rejected due to quota limits or limits on number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.") \
    M(SelectQuery, "Same as Query, but only for SELECT queries.") \
    M(InsertQuery, "Same as Query, but only for INSERT queries.") \
    M(FileOpen, "Number of files opened.") \
    M(Seek, "Number of times the 'lseek' function was called.") \
    M(ReadBufferFromFileDescriptorRead, "Number of reads (read/pread) from a file descriptor. Does not include sockets.") \
    M(ReadBufferFromFileDescriptorReadFailed, "Number of times the read (read/pread) from a file descriptor have failed.") \
    M(ReadBufferFromFileDescriptorReadBytes, "Number of bytes read from file descriptors. If the file is compressed, this will show compressed data size.") \
    M(WriteBufferFromFileDescriptorWrite, "Number of writes (write/pwrite) to a file descriptor. Does not include sockets.") \
    M(WriteBufferFromFileDescriptorWriteFailed, "Number of times the write (write/pwrite) to a file descriptor have failed.") \
    M(WriteBufferFromFileDescriptorWriteBytes, "Number of bytes written to file descriptors. If the file is compressed, this will show compressed data size.") \
    M(ReadBufferAIORead, "") \
    M(ReadBufferAIOReadBytes, "") \
    M(WriteBufferAIOWrite, "") \
    M(WriteBufferAIOWriteBytes, "") \
    M(ReadCompressedBytes, "") \
    M(CompressedReadBufferBlocks, "") \
    M(CompressedReadBufferBytes, "") \
    M(UncompressedCacheHits, "") \
    M(UncompressedCacheMisses, "") \
    M(UncompressedCacheWeightLost, "") \
    M(IOBufferAllocs, "") \
    M(IOBufferAllocBytes, "") \
    M(ArenaAllocChunks, "") \
    M(ArenaAllocBytes, "") \
    M(FunctionExecute, "") \
    M(TableFunctionExecute, "") \
    M(MarkCacheHits, "") \
    M(MarkCacheMisses, "") \
    M(CreatedReadBufferOrdinary, "") \
    M(CreatedReadBufferAIO, "") \
    M(CreatedWriteBufferOrdinary, "") \
    M(CreatedWriteBufferAIO, "") \
    M(DiskReadElapsedMicroseconds, "") \
    M(DiskWriteElapsedMicroseconds, "") \
    M(NetworkReceiveElapsedMicroseconds, "") \
    M(NetworkSendElapsedMicroseconds, "") \
    M(ThrottlerSleepMicroseconds, "") \
    \
    M(ReplicatedPartFetches, "") \
    M(ReplicatedPartFailedFetches, "") \
    M(ObsoleteReplicatedParts, "") \
    M(ReplicatedPartMerges, "") \
    M(ReplicatedPartFetchesOfMerged, "") \
    M(ReplicatedPartMutations, "") \
    M(ReplicatedPartChecks, "") \
    M(ReplicatedPartChecksFailed, "") \
    M(ReplicatedDataLoss, "") \
    \
    M(InsertedRows, "") \
    M(InsertedBytes, "") \
    M(DelayedInserts, "Number of times the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.") \
    M(RejectedInserts, "Number of times the INSERT of a block to a MergeTree table was rejected with 'Too many parts' exception due to high number of active data parts for partition.") \
    M(DelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.") \
    M(DuplicatedInsertedBlocks, "Number of times the INSERTed block to a ReplicatedMergeTree table was deduplicated.") \
    \
    M(ZooKeeperInit, "") \
    M(ZooKeeperTransactions, "") \
    M(ZooKeeperList, "") \
    M(ZooKeeperCreate, "") \
    M(ZooKeeperRemove, "") \
    M(ZooKeeperExists, "") \
    M(ZooKeeperGet, "") \
    M(ZooKeeperSet, "") \
    M(ZooKeeperMulti, "") \
    M(ZooKeeperCheck, "") \
    M(ZooKeeperClose, "") \
    M(ZooKeeperWatchResponse, "") \
    M(ZooKeeperUserExceptions, "") \
    M(ZooKeeperHardwareExceptions, "") \
    M(ZooKeeperOtherExceptions, "") \
    M(ZooKeeperWaitMicroseconds, "") \
    M(ZooKeeperBytesSent, "") \
    M(ZooKeeperBytesReceived, "") \
    \
    M(DistributedConnectionFailTry, "") \
    M(DistributedConnectionMissingTable, "") \
    M(DistributedConnectionStaleReplica, "") \
    M(DistributedConnectionFailAtAll, "") \
    \
    M(CompileAttempt, "") \
    M(CompileSuccess, "") \
    \
    M(CompileFunction, "") \
    \
    M(ExternalSortWritePart, "") \
    M(ExternalSortMerge, "") \
    M(ExternalAggregationWritePart, "") \
    M(ExternalAggregationMerge, "") \
    M(ExternalAggregationCompressedBytes, "") \
    M(ExternalAggregationUncompressedBytes, "") \
    \
    M(SlowRead, "Number of reads from a file that were slow. This indicate system overload. Thresholds are controlled by read_backoff_* settings.") \
    M(ReadBackoff, "Number of times the number of query processing threads was lowered due to slow reads.") \
    \
    M(ReplicaYieldLeadership, "Number of times Replicated table was yielded its leadership due to large replication lag relative to other replicas.") \
    M(ReplicaPartialShutdown, "") \
    \
    M(SelectedParts, "Number of data parts selected to read from a MergeTree table.") \
    M(SelectedRanges, "Number of (non-adjacent) ranges in all data parts selected to read from a MergeTree table.") \
    M(SelectedMarks, "Number of marks (index granules) selected to read from a MergeTree table.") \
    \
    M(MergedRows, "") \
    M(MergedUncompressedBytes, "") \
    M(MergesTimeMilliseconds, "")\
    \
    M(MergeTreeDataWriterRows, "") \
    M(MergeTreeDataWriterUncompressedBytes, "") \
    M(MergeTreeDataWriterCompressedBytes, "") \
    M(MergeTreeDataWriterBlocks, "") \
    M(MergeTreeDataWriterBlocksAlreadySorted, "") \
    \
    M(ObsoleteEphemeralNode, "") \
    M(CannotRemoveEphemeralNode, "") \
    M(LeaderElectionAcquiredLeadership, "") \
    \
    M(RegexpCreated, "Compiled regular expressions. Identical regular expressions compiled just once and cached forever.") \
    M(ContextLock, "Number of times the lock of Context was acquired or tried to acquire. This is global lock.") \
    \
    M(StorageBufferFlush, "") \
    M(StorageBufferErrorOnFlush, "") \
    M(StorageBufferPassedAllMinThresholds, "") \
    M(StorageBufferPassedTimeMaxThreshold, "") \
    M(StorageBufferPassedRowsMaxThreshold, "") \
    M(StorageBufferPassedBytesMaxThreshold, "") \
    \
    M(DictCacheKeysRequested, "") \
    M(DictCacheKeysRequestedMiss, "") \
    M(DictCacheKeysRequestedFound, "") \
    M(DictCacheKeysExpired, "") \
    M(DictCacheKeysNotFound, "") \
    M(DictCacheKeysHit, "") \
    M(DictCacheRequestTimeNs, "") \
    M(DictCacheRequests, "") \
    M(DictCacheLockWriteNs, "") \
    M(DictCacheLockReadNs, "") \
    \
    M(DistributedSyncInsertionTimeoutExceeded, "") \
    M(DataAfterMergeDiffersFromReplica, "") \
    M(DataAfterMutationDiffersFromReplica, "") \
    M(PolygonsAddedToPool, "") \
    M(PolygonsInPoolAllocatedBytes, "") \
    M(RWLockAcquiredReadLocks, "") \
    M(RWLockAcquiredWriteLocks, "") \
    M(RWLockReadersWaitMilliseconds, "") \
    M(RWLockWritersWaitMilliseconds, "") \
    M(NetworkErrors, "") \
    \
    M(RealTimeMicroseconds, "Total (wall clock) time spent in processing (queries and other tasks) threads (not that this is a sum).") \
    M(UserTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in user space. This include time CPU pipeline was stalled due to cache misses, branch mispredictions, hyper-threading, etc.") \
    M(SystemTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in OS kernel space. This include time CPU pipeline was stalled due to cache misses, branch mispredictions, hyper-threading, etc.") \
    M(SoftPageFaults, "") \
    M(HardPageFaults, "") \
    M(VoluntaryContextSwitches, "") \
    M(InvoluntaryContextSwitches, "") \
    \
    M(OSIOWaitMicroseconds, "Total time a thread spent waiting for a result of IO operation, from the OS point of view.") \
    M(OSCPUWaitMicroseconds, "Total time a thread was ready for execution but waiting to be scheduled by OS, from the OS point of view.") \
    M(OSCPUVirtualTimeMicroseconds, "CPU time spent seen by OS. Does not include involuntary waits due to virtualization.") \
    M(OSReadBytes, "Number of bytes read from disks or block devices. Doesn't include bytes read from page cache. May include excessive data due to block size, readahead, etc.") \
    M(OSWriteBytes, "Number of bytes written to disks or block devices. Doesn't include bytes that are in page cache dirty pages. May not include data that was written by OS asynchronously.") \
    M(OSReadChars, "Number of bytes read from filesystem, including page cache.") \
    M(OSWriteChars, "Number of bytes written to filesystem, including page cache.") \


namespace ProfileEvents
{

#define M(NAME, DOCUMENTATION) extern const Event NAME = __COUNTER__;
    APPLY_FOR_EVENTS(M)
#undef M
constexpr Event END = __COUNTER__;

/// Global variable, initialized by zeros.
Counter global_counters_array[END] {};
/// Initialize global counters statically
Counters global_counters(global_counters_array);

const Event Counters::num_counters = END;


Counters::Counters(VariableContext level, Counters * parent)
    : counters_holder(new Counter[num_counters] {}),
      parent(parent),
      level(level)
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
    #define M(NAME, DOCUMENTATION) #NAME,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

const char * getDocumentation(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION) DOCUMENTATION,
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
