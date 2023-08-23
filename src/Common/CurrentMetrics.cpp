#include <Common/CurrentMetrics.h>


/// Available metrics. Add something here as you wish.
#define APPLY_FOR_BUILTIN_METRICS(M) \
    M(Query, "Number of executing queries") \
    M(Merge, "Number of executing background merges") \
    M(Move, "Number of currently executing moves") \
    M(PartMutation, "Number of mutations (ALTER DELETE/UPDATE)") \
    M(ReplicatedFetch, "Number of data parts being fetched from replica") \
    M(ReplicatedSend, "Number of data parts being sent to replicas") \
    M(ReplicatedChecks, "Number of data parts checking for consistency") \
    M(BackgroundMergesAndMutationsPoolTask, "Number of active merges and mutations in an associated background pool") \
    M(BackgroundMergesAndMutationsPoolSize, "Limit on number of active merges and mutations in an associated background pool") \
    M(BackgroundFetchesPoolTask, "Number of active fetches in an associated background pool") \
    M(BackgroundFetchesPoolSize, "Limit on number of simultaneous fetches in an associated background pool") \
    M(BackgroundCommonPoolTask, "Number of active tasks in an associated background pool") \
    M(BackgroundCommonPoolSize, "Limit on number of tasks in an associated background pool") \
    M(BackgroundMovePoolTask, "Number of active tasks in BackgroundProcessingPool for moves") \
    M(BackgroundMovePoolSize, "Limit on number of tasks in BackgroundProcessingPool for moves") \
    M(BackgroundSchedulePoolTask, "Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.") \
    M(BackgroundSchedulePoolSize, "Limit on number of tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.") \
    M(BackgroundBufferFlushSchedulePoolTask, "Number of active tasks in BackgroundBufferFlushSchedulePool. This pool is used for periodic Buffer flushes") \
    M(BackgroundBufferFlushSchedulePoolSize, "Limit on number of tasks in BackgroundBufferFlushSchedulePool") \
    M(BackgroundDistributedSchedulePoolTask, "Number of active tasks in BackgroundDistributedSchedulePool. This pool is used for distributed sends that is done in background.") \
    M(BackgroundDistributedSchedulePoolSize, "Limit on number of tasks in BackgroundDistributedSchedulePool") \
    M(BackgroundMessageBrokerSchedulePoolTask, "Number of active tasks in BackgroundProcessingPool for message streaming") \
    M(BackgroundMessageBrokerSchedulePoolSize, "Limit on number of tasks in BackgroundProcessingPool for message streaming") \
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
    M(TotalTemporaryFiles, "Number of temporary files created") \
    M(TemporaryFilesForSort, "Number of temporary files created for external sorting") \
    M(TemporaryFilesForAggregation, "Number of temporary files created for external aggregation") \
    M(TemporaryFilesForJoin, "Number of temporary files created for JOIN") \
    M(TemporaryFilesUnknown, "Number of temporary files created without known purpose") \
    M(Read, "Number of read (read, pread, io_getevents, etc.) syscalls in fly") \
    M(RemoteRead, "Number of read with remote reader in fly") \
    M(Write, "Number of write (write, pwrite, io_getevents, etc.) syscalls in fly") \
    M(NetworkReceive, "Number of threads receiving data from network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.") \
    M(NetworkSend, "Number of threads sending data to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.") \
    M(SendScalars, "Number of connections that are sending data for scalars to remote servers.") \
    M(SendExternalTables, "Number of connections that are sending data for external tables to remote servers. External tables are used to implement GLOBAL IN and GLOBAL JOIN operators with distributed subqueries.") \
    M(QueryThread, "Number of query processing threads") \
    M(ReadonlyReplica, "Number of Replicated tables that are currently in readonly state due to re-initialization after ZooKeeper session loss or due to startup without ZooKeeper configured.") \
    M(MemoryTracking, "Total amount of memory (bytes) allocated by the server.") \
    M(MergesMutationsMemoryTracking, "Total amount of memory (bytes) allocated by background tasks (merges and mutations).") \
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
    M(MergeTreeDataSelectExecutorThreads, "Number of threads in the MergeTreeDataSelectExecutor thread pool.") \
    M(MergeTreeDataSelectExecutorThreadsActive, "Number of threads in the MergeTreeDataSelectExecutor thread pool running a task.") \
    M(BackupsThreads, "Number of threads in the thread pool for BACKUP.") \
    M(BackupsThreadsActive, "Number of threads in thread pool for BACKUP running a task.") \
    M(RestoreThreads, "Number of threads in the thread pool for RESTORE.") \
    M(RestoreThreadsActive, "Number of threads in the thread pool for RESTORE running a task.") \
    M(MarksLoaderThreads, "Number of threads in thread pool for loading marks.") \
    M(MarksLoaderThreadsActive, "Number of threads in the thread pool for loading marks running a task.") \
    M(IOPrefetchThreads, "Number of threads in the IO prefertch thread pool.") \
    M(IOPrefetchThreadsActive, "Number of threads in the IO prefetch thread pool running a task.") \
    M(IOWriterThreads, "Number of threads in the IO writer thread pool.") \
    M(IOWriterThreadsActive, "Number of threads in the IO writer thread pool running a task.") \
    M(IOThreads, "Number of threads in the IO thread pool.") \
    M(IOThreadsActive, "Number of threads in the IO thread pool running a task.") \
    M(ThreadPoolRemoteFSReaderThreads, "Number of threads in the thread pool for remote_filesystem_read_method=threadpool.") \
    M(ThreadPoolRemoteFSReaderThreadsActive, "Number of threads in the thread pool for remote_filesystem_read_method=threadpool running a task.") \
    M(ThreadPoolFSReaderThreads, "Number of threads in the thread pool for local_filesystem_read_method=threadpool.") \
    M(ThreadPoolFSReaderThreadsActive, "Number of threads in the thread pool for local_filesystem_read_method=threadpool running a task.") \
    M(BackupsIOThreads, "Number of threads in the BackupsIO thread pool.") \
    M(BackupsIOThreadsActive, "Number of threads in the BackupsIO thread pool running a task.") \
    M(DiskObjectStorageAsyncThreads, "Obsolete metric, shows nothing.") \
    M(DiskObjectStorageAsyncThreadsActive, "Obsolete metric, shows nothing.") \
    M(StorageHiveThreads, "Number of threads in the StorageHive thread pool.") \
    M(StorageHiveThreadsActive, "Number of threads in the StorageHive thread pool running a task.") \
    M(TablesLoaderThreads, "Number of threads in the tables loader thread pool.") \
    M(TablesLoaderThreadsActive, "Number of threads in the tables loader thread pool running a task.") \
    M(DatabaseOrdinaryThreads, "Number of threads in the Ordinary database thread pool.") \
    M(DatabaseOrdinaryThreadsActive, "Number of threads in the Ordinary database thread pool running a task.") \
    M(DatabaseOnDiskThreads, "Number of threads in the DatabaseOnDisk thread pool.") \
    M(DatabaseOnDiskThreadsActive, "Number of threads in the DatabaseOnDisk thread pool running a task.") \
    M(DatabaseCatalogThreads, "Number of threads in the DatabaseCatalog thread pool.") \
    M(DatabaseCatalogThreadsActive, "Number of threads in the DatabaseCatalog thread pool running a task.") \
    M(DestroyAggregatesThreads, "Number of threads in the thread pool for destroy aggregate states.") \
    M(DestroyAggregatesThreadsActive, "Number of threads in the thread pool for destroy aggregate states running a task.") \
    M(HashedDictionaryThreads, "Number of threads in the HashedDictionary thread pool.") \
    M(HashedDictionaryThreadsActive, "Number of threads in the HashedDictionary thread pool running a task.") \
    M(CacheDictionaryThreads, "Number of threads in the CacheDictionary thread pool.") \
    M(CacheDictionaryThreadsActive, "Number of threads in the CacheDictionary thread pool running a task.") \
    M(ParallelFormattingOutputFormatThreads, "Number of threads in the ParallelFormattingOutputFormatThreads thread pool.") \
    M(ParallelFormattingOutputFormatThreadsActive, "Number of threads in the ParallelFormattingOutputFormatThreads thread pool running a task.") \
    M(ParallelParsingInputFormatThreads, "Number of threads in the ParallelParsingInputFormat thread pool.") \
    M(ParallelParsingInputFormatThreadsActive, "Number of threads in the ParallelParsingInputFormat thread pool running a task.") \
    M(MergeTreeBackgroundExecutorThreads, "Number of threads in the MergeTreeBackgroundExecutor thread pool.") \
    M(MergeTreeBackgroundExecutorThreadsActive, "Number of threads in the MergeTreeBackgroundExecutor thread pool running a task.") \
    M(AsynchronousInsertThreads, "Number of threads in the AsynchronousInsert thread pool.") \
    M(AsynchronousInsertThreadsActive, "Number of threads in the AsynchronousInsert thread pool running a task.") \
    M(StartupSystemTablesThreads, "Number of threads in the StartupSystemTables thread pool.") \
    M(StartupSystemTablesThreadsActive, "Number of threads in the StartupSystemTables thread pool running a task.") \
    M(AggregatorThreads, "Number of threads in the Aggregator thread pool.") \
    M(AggregatorThreadsActive, "Number of threads in the Aggregator thread pool running a task.") \
    M(DDLWorkerThreads, "Number of threads in the DDLWorker thread pool for ON CLUSTER queries.") \
    M(DDLWorkerThreadsActive, "Number of threads in the DDLWORKER thread pool for ON CLUSTER queries running a task.") \
    M(StorageDistributedThreads, "Number of threads in the StorageDistributed thread pool.") \
    M(StorageDistributedThreadsActive, "Number of threads in the StorageDistributed thread pool running a task.") \
    M(DistributedInsertThreads, "Number of threads used for INSERT into Distributed.") \
    M(DistributedInsertThreadsActive, "Number of threads used for INSERT into Distributed running a task.") \
    M(StorageS3Threads, "Number of threads in the StorageS3 thread pool.") \
    M(StorageS3ThreadsActive, "Number of threads in the StorageS3 thread pool running a task.") \
    M(ObjectStorageS3Threads, "Number of threads in the S3ObjectStorage thread pool.") \
    M(ObjectStorageS3ThreadsActive, "Number of threads in the S3ObjectStorage thread pool running a task.") \
    M(ObjectStorageAzureThreads, "Number of threads in the AzureObjectStorage thread pool.") \
    M(ObjectStorageAzureThreadsActive, "Number of threads in the AzureObjectStorage thread pool running a task.") \
    M(MergeTreePartsLoaderThreads, "Number of threads in the MergeTree parts loader thread pool.") \
    M(MergeTreePartsLoaderThreadsActive, "Number of threads in the MergeTree parts loader thread pool running a task.") \
    M(MergeTreeOutdatedPartsLoaderThreads, "Number of threads in the threadpool for loading Outdated data parts.") \
    M(MergeTreeOutdatedPartsLoaderThreadsActive, "Number of active threads in the threadpool for loading Outdated data parts.") \
    M(MergeTreePartsCleanerThreads, "Number of threads in the MergeTree parts cleaner thread pool.") \
    M(MergeTreePartsCleanerThreadsActive, "Number of threads in the MergeTree parts cleaner thread pool running a task.") \
    M(IDiskCopierThreads, "Number of threads for copying data between disks of different types.") \
    M(IDiskCopierThreadsActive, "Number of threads for copying data between disks of different types running a task.") \
    M(SystemReplicasThreads, "Number of threads in the system.replicas thread pool.") \
    M(SystemReplicasThreadsActive, "Number of threads in the system.replicas thread pool running a task.") \
    M(RestartReplicaThreads, "Number of threads in the RESTART REPLICA thread pool.") \
    M(RestartReplicaThreadsActive, "Number of threads in the RESTART REPLICA thread pool running a task.") \
    M(QueryPipelineExecutorThreads, "Number of threads in the PipelineExecutor thread pool.") \
    M(QueryPipelineExecutorThreadsActive, "Number of threads in the PipelineExecutor thread pool running a task.") \
    M(ParquetDecoderThreads, "Number of threads in the ParquetBlockInputFormat thread pool.") \
    M(ParquetDecoderThreadsActive, "Number of threads in the ParquetBlockInputFormat thread pool running a task.") \
    M(ParquetEncoderThreads, "Number of threads in ParquetBlockOutputFormat thread pool.") \
    M(ParquetEncoderThreadsActive, "Number of threads in ParquetBlockOutputFormat thread pool running a task.") \
    M(OutdatedPartsLoadingThreads, "Number of threads in the threadpool for loading Outdated data parts.") \
    M(OutdatedPartsLoadingThreadsActive, "Number of active threads in the threadpool for loading Outdated data parts.") \
    M(DistributedBytesToInsert, "Number of pending bytes to process for asynchronous insertion into Distributed tables. Number of bytes for every shard is summed.") \
    M(BrokenDistributedBytesToInsert, "Number of bytes for asynchronous insertion into Distributed tables that has been marked as broken. Number of bytes for every shard is summed.") \
    M(DistributedFilesToInsert, "Number of pending files to process for asynchronous insertion into Distributed tables. Number of files for every shard is summed.") \
    M(BrokenDistributedFilesToInsert, "Number of files for asynchronous insertion into Distributed tables that has been marked as broken. Number of files for every shard is summed.") \
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
    M(MMappedAllocs, "Total number of mmapped allocations") \
    M(MMappedAllocBytes, "Sum bytes of mmapped allocations") \
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
    M(FilesystemCacheSize, "Filesystem cache size in bytes") \
    M(FilesystemCacheSizeLimit, "Filesystem cache size limit in bytes") \
    M(FilesystemCacheElements, "Filesystem cache elements (file segments)") \
    M(FilesystemCacheDownloadQueueElements, "Filesystem cache elements in download queue") \
    M(FilesystemCacheDelayedCleanupElements, "Filesystem cache elements in background cleanup queue") \
    M(AsyncInsertCacheSize, "Number of async insert hash id in cache") \
    M(S3Requests, "S3 requests") \
    M(KeeperAliveConnections, "Number of alive connections") \
    M(KeeperOutstandingRequets, "Number of outstanding requests") \
    M(ThreadsInOvercommitTracker, "Number of waiting threads inside of OvercommitTracker") \
    M(IOUringPendingEvents, "Number of io_uring SQEs waiting to be submitted") \
    M(IOUringInFlightEvents, "Number of io_uring SQEs in flight") \
    M(ReadTaskRequestsSent, "The current number of callback requests in flight from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the remote server side.") \
    M(MergeTreeReadTaskRequestsSent, "The current number of callback requests in flight from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the remote server side.") \
    M(MergeTreeAllRangesAnnouncementsSent, "The current number of announcement being sent in flight from the remote server to the initiator server about the set of data parts (for MergeTree tables). Measured on the remote server side.") \
    M(CreatedTimersInQueryProfiler, "Number of Created thread local timers in QueryProfiler") \
    M(ActiveTimersInQueryProfiler, "Number of Active thread local timers in QueryProfiler") \

#ifdef APPLY_FOR_EXTERNAL_METRICS
    #define APPLY_FOR_METRICS(M) APPLY_FOR_BUILTIN_METRICS(M) APPLY_FOR_EXTERNAL_METRICS(M)
#else
    #define APPLY_FOR_METRICS(M) APPLY_FOR_BUILTIN_METRICS(M)
#endif

namespace CurrentMetrics
{
    #define M(NAME, DOCUMENTATION) extern const Metric NAME = Metric(__COUNTER__);
        APPLY_FOR_METRICS(M)
    #undef M
    constexpr Metric END = Metric(__COUNTER__);

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
