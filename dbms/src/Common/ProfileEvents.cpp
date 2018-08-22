#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnArray.h>

/// Available events. Add something here as you wish.
#define APPLY_FOR_EVENTS(M) \
    M(Query) \
    M(SelectQuery) \
    M(InsertQuery) \
    M(FileOpen) \
    M(FileOpenFailed) \
    M(Seek) \
    M(ReadBufferFromFileDescriptorRead) \
    M(ReadBufferFromFileDescriptorReadFailed) \
    M(ReadBufferFromFileDescriptorReadBytes) \
    M(WriteBufferFromFileDescriptorWrite) \
    M(WriteBufferFromFileDescriptorWriteFailed) \
    M(WriteBufferFromFileDescriptorWriteBytes) \
    M(ReadBufferAIORead) \
    M(ReadBufferAIOReadBytes) \
    M(WriteBufferAIOWrite) \
    M(WriteBufferAIOWriteBytes) \
    M(ReadCompressedBytes) \
    M(CompressedReadBufferBlocks) \
    M(CompressedReadBufferBytes) \
    M(UncompressedCacheHits) \
    M(UncompressedCacheMisses) \
    M(UncompressedCacheWeightLost) \
    M(IOBufferAllocs) \
    M(IOBufferAllocBytes) \
    M(ArenaAllocChunks) \
    M(ArenaAllocBytes) \
    M(FunctionExecute) \
    M(TableFunctionExecute) \
    M(MarkCacheHits) \
    M(MarkCacheMisses) \
    M(CreatedReadBufferOrdinary) \
    M(CreatedReadBufferAIO) \
    M(CreatedWriteBufferOrdinary) \
    M(CreatedWriteBufferAIO) \
    M(DiskReadElapsedMicroseconds) \
    M(DiskWriteElapsedMicroseconds) \
    M(NetworkReceiveElapsedMicroseconds) \
    M(NetworkSendElapsedMicroseconds) \
    M(ThrottlerSleepMicroseconds) \
    \
    M(ReplicatedPartFetches) \
    M(ReplicatedPartFailedFetches) \
    M(ObsoleteReplicatedParts) \
    M(ReplicatedPartMerges) \
    M(ReplicatedPartFetchesOfMerged) \
    M(ReplicatedPartMutations) \
    M(ReplicatedPartChecks) \
    M(ReplicatedPartChecksFailed) \
    M(ReplicatedDataLoss) \
    \
    M(InsertedRows) \
    M(InsertedBytes) \
    M(DelayedInserts) \
    M(RejectedInserts) \
    M(DelayedInsertsMilliseconds) \
    M(DuplicatedInsertedBlocks) \
    \
    M(ZooKeeperInit) \
    M(ZooKeeperTransactions) \
    M(ZooKeeperList) \
    M(ZooKeeperCreate) \
    M(ZooKeeperRemove) \
    M(ZooKeeperExists) \
    M(ZooKeeperGet) \
    M(ZooKeeperSet) \
    M(ZooKeeperMulti) \
    M(ZooKeeperCheck) \
    M(ZooKeeperClose) \
    M(ZooKeeperWatchResponse) \
    M(ZooKeeperUserExceptions) \
    M(ZooKeeperHardwareExceptions) \
    M(ZooKeeperOtherExceptions) \
    M(ZooKeeperWaitMicroseconds) \
    M(ZooKeeperBytesSent) \
    M(ZooKeeperBytesReceived) \
    \
    M(DistributedConnectionFailTry) \
    M(DistributedConnectionMissingTable) \
    M(DistributedConnectionStaleReplica) \
    M(DistributedConnectionFailAtAll) \
    \
    M(CompileAttempt) \
    M(CompileSuccess) \
    \
    M(CompileFunction) \
    \
    M(ExternalSortWritePart) \
    M(ExternalSortMerge) \
    M(ExternalAggregationWritePart) \
    M(ExternalAggregationMerge) \
    M(ExternalAggregationCompressedBytes) \
    M(ExternalAggregationUncompressedBytes) \
    \
    M(SlowRead) \
    M(ReadBackoff) \
    \
    M(ReplicaYieldLeadership) \
    M(ReplicaPartialShutdown) \
    \
    M(SelectedParts) \
    M(SelectedRanges) \
    M(SelectedMarks) \
    \
    M(MergedRows) \
    M(MergedUncompressedBytes) \
    M(MergesTimeMilliseconds)\
    \
    M(MergeTreeDataWriterRows) \
    M(MergeTreeDataWriterUncompressedBytes) \
    M(MergeTreeDataWriterCompressedBytes) \
    M(MergeTreeDataWriterBlocks) \
    M(MergeTreeDataWriterBlocksAlreadySorted) \
    \
    M(ObsoleteEphemeralNode) \
    M(CannotRemoveEphemeralNode) \
    M(LeaderElectionAcquiredLeadership) \
    \
    M(RegexpCreated) \
    M(ContextLock) \
    \
    M(StorageBufferFlush) \
    M(StorageBufferErrorOnFlush) \
    M(StorageBufferPassedAllMinThresholds) \
    M(StorageBufferPassedTimeMaxThreshold) \
    M(StorageBufferPassedRowsMaxThreshold) \
    M(StorageBufferPassedBytesMaxThreshold) \
    \
    M(DictCacheKeysRequested) \
    M(DictCacheKeysRequestedMiss) \
    M(DictCacheKeysRequestedFound) \
    M(DictCacheKeysExpired) \
    M(DictCacheKeysNotFound) \
    M(DictCacheKeysHit) \
    M(DictCacheRequestTimeNs) \
    M(DictCacheRequests) \
    M(DictCacheLockWriteNs) \
    M(DictCacheLockReadNs) \
    \
    M(DistributedSyncInsertionTimeoutExceeded) \
    M(DataAfterMergeDiffersFromReplica) \
    M(DataAfterMutationDiffersFromReplica) \
    M(PolygonsAddedToPool) \
    M(PolygonsInPoolAllocatedBytes) \
    M(RWLockAcquiredReadLocks) \
    M(RWLockAcquiredWriteLocks) \
    M(RWLockReadersWaitMilliseconds) \
    M(RWLockWritersWaitMilliseconds) \
    M(NetworkErrors) \
    \
    M(RealTimeMicroseconds) \
    M(UserTimeMicroseconds) \
    M(SystemTimeMicroseconds) \
    M(SoftPageFaults) \
    M(HardPageFaults) \
    M(VoluntaryContextSwitches) \
    M(InvoluntaryContextSwitches) \
    \
    M(OSIOWaitMicroseconds) \
    M(OSCPUWaitMicroseconds) \
    M(OSCPUVirtualTimeMicroseconds) \
    M(OSReadBytes) \
    M(OSWriteBytes) \
    M(OSReadChars) \
    M(OSWriteChars) \


namespace ProfileEvents
{

#define M(NAME) extern const Event NAME = __COUNTER__;
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

const char * getDescription(Event event)
{
    static const char * descriptions[] =
    {
    #define M(NAME) #NAME,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return descriptions[event];
}


Event end() { return END; }


void increment(Event event, Count amount)
{
    DB::CurrentThread::getProfileEvents().increment(event, amount);
}

}

#undef APPLY_FOR_EVENTS
