#include <Common/CurrentMetrics.h>


/// Available metrics. Add something here as you wish.
#define APPLY_FOR_METRICS(M) \
    M(Query, "Number of executing queries") \
    M(Merge, "Number of executing background merges") \
    M(PartMutation, "Number of mutations (ALTER DELETE/UPDATE)") \
    M(ReplicatedFetch, "Number of data parts being fetched from replica") \
    M(ReplicatedSend, "Number of data parts being sent to replicas") \
    M(ReplicatedChecks, "Number of data parts checking for consistency") \
    M(BackgroundMergesAndMutationsPoolTask, "Number of active merges and mutations in an associated background pool") \
    M(BackgroundFetchesPoolTask, "Number of active fetches in an associated background pool") \
    M(BackgroundCommonPoolTask, "Number of active tasks in an associated background pool") \
    M(BackgroundMaintPoolTask, "Number of active tasks in BackgroundProcessingPool (maint)") \
    M(BackgroundMovePoolTask, "Number of active tasks in BackgroundProcessingPool for moves") \
    M(BackgroundSchedulePoolTask, "Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.") \
    M(BackgroundBufferFlushSchedulePoolTask, "Number of active tasks in BackgroundBufferFlushSchedulePool. This pool is used for periodic Buffer flushes") \
    M(BackgroundDistributedSchedulePoolTask, "Number of active tasks in BackgroundDistributedSchedulePool. This pool is used for distributed sends that is done in background.") \
    M(BackgroundMessageBrokerSchedulePoolTask, "Number of active tasks in BackgroundProcessingPool for message streaming") \
    M(CacheDictionaryUpdateQueueBatches, "Number of 'batches' (a set of keys) in update queue in CacheDictionaries.") \
    M(CacheDictionaryUpdateQueueKeys, "Exact number of keys in update queue in CacheDictionaries.") \
    M(DiskSpaceReservedForMerge, "Disk space reserved for currently running background merges. It is slightly more than the total size of currently merging parts.") \
    M(DistributedSend, "Number of connections to remote servers sending data that was INSERTed into Distributed tables. Both synchronous and asynchronous mode.") \
    M(QueryPreempted, "Number of queries that are stopped and waiting due to 'priority' setting.") \
    M(TCPConnection, "Number of connections to TCP server (clients with native interface), also included server-server distributed query connections") \
    M(MySQLConnection, "Number of client connections using MySQL protocol") \
    M(HTTPConnection, "Number of connections to HTTP server") \
    M(InterserverConnection, "Number of connections from other replicas to fetch parts") \
    M(PostgreSQLConnection, "Number of client connections using PostgreSQL protocol") \
    M(OpenFileForRead, "Number of files open for reading") \
    M(OpenFileForWrite, "Number of files open for writing") \
    M(Read, "Number of read (read, pread, io_getevents, etc.) syscalls in fly") \
    M(Write, "Number of write (write, pwrite, io_getevents, etc.) syscalls in fly") \
    M(NetworkReceive, "Number of threads receiving data from network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.") \
    M(NetworkSend, "Number of threads sending data to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.") \
    M(SendScalars, "Number of connections that are sending data for scalars to remote servers.") \
    M(SendExternalTables, "Number of connections that are sending data for external tables to remote servers. External tables are used to implement GLOBAL IN and GLOBAL JOIN operators with distributed subqueries.") \
    M(QueryThread, "Number of query processing threads") \
    M(ReadonlyReplica, "Number of Replicated tables that are currently in readonly state due to re-initialization after ZooKeeper session loss or due to startup without ZooKeeper configured.") \
    M(MemoryTracking, "Total amount of memory (bytes) allocated by the server.") \
    M(EphemeralNode, "Number of ephemeral nodes hold in ZooKeeper.") \
    M(ZooKeeperSession, "Number of sessions (connections) to ZooKeeper. Should be no more than one, because using more than one connection to ZooKeeper may lead to bugs due to lack of linearizability (stale reads) that ZooKeeper consistency model allows.") \
    M(ZooKeeperWatch, "Number of watches (event subscriptions) in ZooKeeper.") \
    M(ZooKeeperRequest, "Number of requests to ZooKeeper in fly.") \
    M(DelayedInserts, "Number of INSERT queries that are throttled due to high number of active data parts for partition in a MergeTree table.") \
    M(ContextLockWait, "Number of threads waiting for lock in Context. This is global lock.") \
    M(StorageBufferRows, "Number of rows in buffers of Buffer tables") \
    M(StorageBufferBytes, "Number of bytes in buffers of Buffer tables") \
    M(DictCacheRequests, "Number of requests in fly to data sources of dictionaries of cache type.") \
    M(Revision, "Revision of the server. It is a number incremented for every release or release candidate except patch releases.") \
    M(VersionInteger, "Version of the server in a single integer number in base-1000. For example, version 11.22.33 is translated to 11022033.") \
    M(RWLockWaitingReaders, "Number of threads waiting for read on a table RWLock.") \
    M(RWLockWaitingWriters, "Number of threads waiting for write on a table RWLock.") \
    M(RWLockActiveReaders, "Number of threads holding read lock in a table RWLock.") \
    M(RWLockActiveWriters, "Number of threads holding write lock in a table RWLock.") \
    M(GlobalThread, "Number of threads in global thread pool.") \
    M(GlobalThreadActive, "Number of threads in global thread pool running a task.") \
    M(LocalThread, "Number of threads in local thread pools. The threads in local thread pools are taken from the global thread pool.") \
    M(LocalThreadActive, "Number of threads in local thread pools running a task.") \
    M(DistributedFilesToInsert, "Number of pending files to process for asynchronous insertion into Distributed tables. Number of files for every shard is summed.") \
    M(BrokenDistributedFilesToInsert, "Number of files for asynchronous insertion into Distributed tables that has been marked as broken. This metric will starts from 0 on start. Number of files for every shard is summed.") \
    M(TablesToDropQueueSize, "Number of dropped tables, that are waiting for background data removal.") \
    M(MaxDDLEntryID, "Max processed DDL entry of DDLWorker.") \
    M(MaxPushedDDLEntryID, "Max DDL entry of DDLWorker that pushed to zookeeper.") \
    M(PartsTemporary, "The part is generating now, it is not in data_parts list.") \
    M(PartsPreCommitted, "Deprecated. See PartsPreActive.") \
    M(PartsCommitted, "Deprecated. See PartsActive.") \
    M(PartsPreActive, "The part is in data_parts, but not used for SELECTs.") \
    M(PartsActive, "Active data part, used by current and upcoming SELECTs.") \
    M(PartsOutdated, "Not active data part, but could be used by only current SELECTs, could be deleted after SELECTs finishes.") \
    M(PartsDeleting, "Not active data part with identity refcounter, it is deleting right now by a cleaner.") \
    M(PartsDeleteOnDestroy, "Part was moved to another disk and should be deleted in own destructor.") \
    M(PartsWide, "Wide parts.") \
    M(PartsCompact, "Compact parts.") \
    M(PartsInMemory, "In-memory parts.") \
    M(MMappedFiles, "Total number of mmapped files.") \
    M(MMappedFileBytes, "Sum size of mmapped file regions.") \
    M(AsyncDrainedConnections, "Number of connections drained asynchronously.") \
    M(ActiveAsyncDrainedConnections, "Number of active connections drained asynchronously.") \
    M(SyncDrainedConnections, "Number of connections drained synchronously.") \
    M(ActiveSyncDrainedConnections, "Number of active connections drained synchronously.") \
    M(AsynchronousReadWait, "Number of threads waiting for asynchronous read.") \
    M(PendingAsyncInsert, "Number of asynchronous inserts that are waiting for flush.") \
    M(KafkaConsumers, "Number of active Kafka consumers") \
    M(KafkaConsumersWithAssignment, "Number of active Kafka consumers which have some partitions assigned.") \
    M(KafkaProducers, "Number of active Kafka producer created") \
    M(KafkaLibrdkafkaThreads, "Number of active librdkafka threads") \
    M(KafkaBackgroundReads, "Number of background reads currently working (populating materialized views from Kafka)") \
    M(KafkaConsumersInUse, "Number of consumers which are currently used by direct or background reads") \
    M(KafkaWrites, "Number of currently running inserts to Kafka") \
    M(KafkaAssignedPartitions, "Number of partitions Kafka tables currently assigned to") \
    M(FilesystemCacheReadBuffers, "Number of active cache buffers") \
    M(CacheFileSegments, "Number of existing cache file segments") \
    M(CacheDetachedFileSegments, "Number of existing detached cache file segments") \
    M(S3Requests, "S3 requests") \
    M(KeeperAliveConnections, "Number of alive connections") \
    M(KeeperOutstandingRequets, "Number of outstanding requests") \

namespace CurrentMetrics
{
    #define M(NAME, DOCUMENTATION) extern const Metric NAME = __COUNTER__;
        APPLY_FOR_METRICS(M)
    #undef M
    constexpr Metric END = __COUNTER__;

    std::atomic<Value> values[END] {};    /// Global variable, initialized by zeros.

    const char * getName(Metric event)
    {
        static const char * strings[] =
        {
        #define M(NAME, DOCUMENTATION) #NAME,
            APPLY_FOR_METRICS(M)
        #undef M
        };

        return strings[event];
    }

    const char * getDocumentation(Metric event)
    {
        static const char * strings[] =
        {
        #define M(NAME, DOCUMENTATION) DOCUMENTATION,
            APPLY_FOR_METRICS(M)
        #undef M
        };

        return strings[event];
    }

    Metric end() { return END; }
}

#undef APPLY_FOR_METRICS
