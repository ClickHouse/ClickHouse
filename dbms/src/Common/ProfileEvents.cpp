#include <Common/ProfileEvents.h>


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
    \
    M(ReplicatedPartFetches) \
    M(ReplicatedPartFailedFetches) \
    M(ObsoleteReplicatedParts) \
    M(ReplicatedPartMerges) \
    M(ReplicatedPartFetchesOfMerged) \
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
    M(ZooKeeperGetChildren) \
    M(ZooKeeperCreate) \
    M(ZooKeeperRemove) \
    M(ZooKeeperExists) \
    M(ZooKeeperGet) \
    M(ZooKeeperSet) \
    M(ZooKeeperMulti) \
    M(ZooKeeperExceptions) \
    \
    M(DistributedConnectionFailTry) \
    M(DistributedConnectionMissingTable) \
    M(DistributedConnectionStaleReplica) \
    M(DistributedConnectionFailAtAll) \
    \
    M(CompileAttempt) \
    M(CompileSuccess) \
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
    M(PolygonsAddedToPool) \
    M(PolygonsInPoolAllocatedBytes) \
    M(RWLockAcquiredReadLocks) \
    M(RWLockAcquiredWriteLocks) \
    M(RWLockReadersWaitMilliseconds) \
    M(RWLockWritersWaitMilliseconds)

namespace ProfileEvents
{
    #define M(NAME) extern const Event NAME = __COUNTER__;
        APPLY_FOR_EVENTS(M)
    #undef M
    constexpr Event END = __COUNTER__;

    std::atomic<Count> counters[END] {};    /// Global variable, initialized by zeros.

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
}

#undef APPLY_FOR_EVENTS
