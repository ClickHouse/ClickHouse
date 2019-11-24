#include <Common/CurrentMetrics.h>


/// Available metrics. Add something here as you wish.
#define APPLY_FOR_METRICS(M) \
    M(Query, \
      "queries", \
      "Number of executing queries") \
    M(Merge, \
      "merges", \
      "Number of executing background merges") \
    M(PartMutation, \
      "part_mutations", \
      "Number of mutations (ALTER DELETE/UPDATE)") \
    M(ReplicatedFetch, \
      "replicated_fetch", \
      "Number of data parts being fetched from replica") \
    M(ReplicatedSend, \
      "replicated_send", \
      "Number of data parts being sent to replicas") \
    M(ReplicatedChecks, \
      "replicated_checks", \
      "Number of data parts checking for consistency") \
    M(BackgroundPoolTask, \
      "background_pool_task", \
      "Number of active tasks in BackgroundProcessingPool (merges, mutations, fetches, or replication queue bookkeeping)") \
    M(BackgroundSchedulePoolTask, \
      "background_schedule_pool_task", \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.") \
    M(DiskSpaceReservedForMerge, \
      "disk_space_reserved_for_merge_bytes", \
      "Disk space reserved for currently running background merges. It is slightly more than the total size of currently merging parts.") \
    M(DistributedSend, \
      "distributed_send", \
      "Number of connections to remote servers sending data that was INSERTed into Distributed tables. Both synchronous and asynchronous mode.") \
    M(QueryPreempted, \
      "query_preempted", \
      "Number of queries that are stopped and waiting due to 'priority' setting.") \
    M(TCPConnection, \
      "tcp_connection", \
      "Number of connections to TCP server (clients with native interface)") \
    M(HTTPConnection, \
      "http_connection", \
      "Number of connections to HTTP server") \
    M(InterserverConnection, \
      "interserver_connection", \
      "Number of connections from other replicas to fetch parts") \
    M(OpenFileForRead, \
      "open_file_for_read", \
      "Number of files open for reading") \
    M(OpenFileForWrite, \
      "open_file_for_write", \
      "Number of files open for writing") \
    M(Read, \
      "reads", \
      "Number of read (read, pread, io_getevents, etc.) syscalls in fly") \
    M(Write, \
      "writes", \
      "Number of write (write, pwrite, io_getevents, etc.) syscalls in fly") \
    M(SendScalars, \
      "send_scalars", \
      "Number of connections that are sending data for scalars to remote servers.") \
    M(SendExternalTables, \
      "send_external_tables", \
      "Number of connections that are sending data for external tables to remote servers. External tables are used to implement GLOBAL IN and GLOBAL JOIN operators with distributed subqueries.") \
    M(QueryThread, \
      "query_thread", \
      "Number of query processing threads") \
    M(ReadonlyReplica, \
      "readonly_replica", \
      "Number of Replicated tables that are currently in readonly state due to re-initialization after ZooKeeper session loss or due to startup without ZooKeeper configured.") \
    M(LeaderReplica, \
      "leader_replica", \
      "Number of Replicated tables that are leaders. Leader replica is responsible for assigning merges, cleaning old blocks for deduplications and a few more bookkeeping tasks. There may be no more than one leader across all replicas at one moment of time. If there is no leader it will be elected soon or it indicate an issue.") \
    M(MemoryTracking, \
      "memory_tracking", \
      "Total amount of memory (bytes) allocated in currently executing queries. Note that some memory allocations may not be accounted.") \
    M(MemoryTrackingInBackgroundProcessingPool, \
      "memory_tracking_in_background_processing_pool_bytes", \
      "Total amount of memory (bytes) allocated in background processing pool (that is dedicated for backround merges, mutations and fetches). Note that this value may include a drift when the memory was allocated in a context of background processing pool and freed in other context or vice-versa. This happens naturally due to caches for tables indexes and doesn't indicate memory leaks.") \
    M(MemoryTrackingInBackgroundSchedulePool, \
      "memory_tracking_in_background_schedule_pool_bytes", \
      "Total amount of memory (bytes) allocated in background schedule pool (that is dedicated for bookkeeping tasks of Replicated tables).") \
    M(MemoryTrackingForMerges, \
      "memory_tracking_for_merges_bytes", \
      "Total amount of memory (bytes) allocated for background merges. Included in MemoryTrackingInBackgroundProcessingPool. Note that this value may include a drift when the memory was allocated in a context of background processing pool and freed in other context or vice-versa. This happens naturally due to caches for tables indexes and doesn't indicate memory leaks.") \
    M(LeaderElection, \
      "leader_election", \
      "Number of Replicas participating in leader election. Equals to total number of replicas in usual cases.") \
    M(EphemeralNode, \
      "ephemeral_nodes", \
      "Number of ephemeral nodes hold in ZooKeeper.") \
    M(ZooKeeperSession, \
      "zoo_keeper_sessions", \
      "Number of sessions (connections) to ZooKeeper. Should be no more than one, because using more than one connection to ZooKeeper may lead to bugs due to lack of linearizability (stale reads) that ZooKeeper consistency model allows.") \
    M(ZooKeeperWatch, \
      "zoo_keeper_watches", \
      "Number of watches (event subscriptions) in ZooKeeper.") \
    M(ZooKeeperRequest, \
      "zoo_keeper_requests", \
      "Number of requests to ZooKeeper in fly.") \
    M(DelayedInserts, \
      "delayed_inserts", \
      "Number of INSERT queries that are throttled due to high number of active data parts for partition in a MergeTree table.") \
    M(ContextLockWait, \
      "context_lock_wait", \
      "Number of threads waiting for lock in Context. This is global lock.") \
    M(StorageBufferRows, \
      "storage_buffer_rows", \
      "Number of rows in buffers of Buffer tables") \
    M(StorageBufferBytes, \
      "storage_buffer_bytes", \
      "Number of bytes in buffers of Buffer tables") \
    M(DictCacheRequests, \
      "dict_cache_requests", \
      "Number of requests in fly to data sources of dictionaries of cache type.") \
    M(Revision, \
      "revisions", \
      "Revision of the server. It is a number incremented for every release or release candidate except patch releases.") \
    M(VersionInteger, \
      "version_integer", \
      "Version of the server in a single integer number in base-1000. For example, version 11.22.33 is translated to 11022033.") \
    M(RWLockWaitingReaders, \
      "rw_lock_waiting_readers", \
      "Number of threads waiting for read on a table RWLock.") \
    M(RWLockWaitingWriters, \
      "rw_lock_waiting_writers", \
      "Number of threads waiting for write on a table RWLock.") \
    M(RWLockActiveReaders, \
      "rw_lock_active_readers", \
      "Number of threads holding read lock in a table RWLock.") \
    M(RWLockActiveWriters, \
      "rw_lock_active_writers", \
      "Number of threads holding write lock in a table RWLock.") \
    M(GlobalThread, \
      "global_threads", \
      "Number of threads in global thread pool.") \
    M(GlobalThreadActive, \
      "global_threads_active", \
      "Number of threads in global thread pool running a task.") \
    M(LocalThread, \
      "local_threads", \
      "Number of threads in local thread pools. Should be similar to GlobalThreadActive.") \
    M(LocalThreadActive, \
      "local_threads_active", \
      "Number of threads in local thread pools running a task.") \
    M(DistributedFilesToInsert, \
      "distributed_files_to_insert", \
      "Number of pending files to process for asynchronous insertion into Distributed tables. Number of files for every shard is summed.") \


namespace CurrentMetrics
{
    #define M(NAME, SNAKE_NAME, DOCUMENTATION) extern const Metric NAME = __COUNTER__;
        APPLY_FOR_METRICS(M)
    #undef M
    constexpr Metric END = __COUNTER__;

    std::atomic<Value> values[END] {};    /// Global variable, initialized by zeros.

    const char * getName(Metric event)
    {
        static const char * strings[] =
        {
        #define M(NAME, SNAKE_NAME, DOCUMENTATION) #NAME,
            APPLY_FOR_METRICS(M)
        #undef M
        };

        return strings[event];
    }

    const char * getNameSnake(Metric event)
    {
        static const char * strings[] =
        {
        #define M(NAME, SNAKE_NAME, DOCUMENTATION) SNAKE_NAME,
            APPLY_FOR_METRICS(M)
        #undef M
        };

        return strings[event];
    }

    const char * getDocumentation(Metric event)
    {
        static const char * strings[] =
        {
        #define M(NAME, SNAKE_NAME, DOCUMENTATION) DOCUMENTATION,
            APPLY_FOR_METRICS(M)
        #undef M
        };

        return strings[event];
    }

    Metric end() { return END; }
}

#undef APPLY_FOR_METRICS
