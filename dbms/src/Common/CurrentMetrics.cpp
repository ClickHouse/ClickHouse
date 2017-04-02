#include <Common/CurrentMetrics.h>


/// Available metrics. Add something here as you wish.
#define APPLY_FOR_METRICS(M) \
    M(Query) \
    M(Merge) \
    M(ReplicatedFetch) \
    M(ReplicatedSend) \
    M(ReplicatedChecks) \
    M(BackgroundPoolTask) \
    M(DiskSpaceReservedForMerge) \
    M(DistributedSend) \
    M(QueryPreempted) \
    M(TCPConnection) \
    M(HTTPConnection) \
    M(InterserverConnection) \
    M(OpenFileForRead) \
    M(OpenFileForWrite) \
    M(Read) \
    M(Write) \
    M(SendExternalTables) \
    M(QueryThread) \
    M(ReadonlyReplica) \
    M(LeaderReplica) \
    M(MemoryTracking) \
    M(MemoryTrackingInBackgroundProcessingPool) \
    M(MemoryTrackingForMerges) \
    M(LeaderElection) \
    M(EphemeralNode) \
    M(ZooKeeperWatch) \
    M(DelayedInserts) \
    M(ContextLockWait) \
    M(StorageBufferRows) \
    M(StorageBufferBytes) \
    M(DictCacheRequests) \


namespace CurrentMetrics
{
    #define M(NAME) extern const Metric NAME = __COUNTER__;
        APPLY_FOR_METRICS(M)
    #undef M
    constexpr Metric END = __COUNTER__;

    std::atomic<Value> values[END] {};    /// Global variable, initialized by zeros.

    const char * getDescription(Metric event)
    {
        static const char * descriptions[] =
        {
        #define M(NAME) #NAME,
            APPLY_FOR_METRICS(M)
        #undef M
        };

        return descriptions[event];
    }

    Metric end() { return END; }
}

#undef APPLY_FOR_METRICS
