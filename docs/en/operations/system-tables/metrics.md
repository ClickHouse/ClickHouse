---
description: 'System table containing metrics which can be calculated instantly, or
  have a current value.'
keywords: ['system table', 'metrics']
slug: /operations/system-tables/metrics
title: 'system.metrics'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.metrics

<SystemTableCloud/>

Contains metrics which can be calculated instantly, or have a current value. For example, the number of simultaneously processed queries or the current replica delay. This table is always up to date.

Columns:

- `metric` ([String](../../sql-reference/data-types/string.md)) — Metric name.
- `value` ([Int64](../../sql-reference/data-types/int-uint.md)) — Metric value.
- `description` ([String](../../sql-reference/data-types/string.md)) — Metric description.
- `name` ([String](../../sql-reference/data-types/string.md)) — Alias for `metric`.

You can find all supported metrics in source file [src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp).

**Example**

```sql
SELECT * FROM system.metrics LIMIT 10
```

```text
┌─metric───────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────┐
│ Query                                │     1 │ Number of executing queries                                            │
│ Merge                                │     0 │ Number of executing background merges                                  │
│ PartMutation                         │     0 │ Number of mutations (ALTER DELETE/UPDATE)                              │
│ ReplicatedFetch                      │     0 │ Number of data parts being fetched from replicas                       │
│ ReplicatedSend                       │     0 │ Number of data parts being sent to replicas                            │
│ ReplicatedChecks                     │     0 │ Number of data parts checking for consistency                          │
│ BackgroundMergesAndMutationsPoolTask │     0 │ Number of active merges and mutations in an associated background pool │
│ BackgroundFetchesPoolTask            │     0 │ Number of active fetches in an associated background pool              │
│ BackgroundCommonPoolTask             │     0 │ Number of active tasks in an associated background pool                │
│ BackgroundMovePoolTask               │     0 │ Number of active tasks in BackgroundProcessingPool for moves           │
└──────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────┘
```

## Metric descriptions {#metric-descriptions}

### AggregatorThreads {#aggregatorthreads}

Number of threads in the Aggregator thread pool.

### AggregatorThreadsActive {#aggregatorthreadsactive}

Number of threads in the Aggregator thread pool running a task.

### TablesLoaderForegroundThreads {#tablesloaderforegroundthreads}

Number of threads in the async loader foreground thread pool.

### TablesLoaderForegroundThreadsActive {#tablesloaderforegroundthreadsactive}

Number of threads in the async loader foreground thread pool running a task.

### TablesLoaderBackgroundThreads {#tablesloaderbackgroundthreads}

Number of threads in the async loader background thread pool.

### TablesLoaderBackgroundThreadsActive {#tablesloaderbackgroundthreadsactive}

Number of threads in the async loader background thread pool running a task.

### AsyncInsertCacheSize {#asyncinsertcachesize}

Number of async insert hash id in cache

### AsynchronousInsertThreads {#asynchronousinsertthreads}

Number of threads in the AsynchronousInsert thread pool.

### AsynchronousInsertThreadsActive {#asynchronousinsertthreadsactive}

Number of threads in the AsynchronousInsert thread pool running a task.

### AsynchronousReadWait {#asynchronousreadwait}

Number of threads waiting for asynchronous read.

### BackgroundBufferFlushSchedulePoolSize {#backgroundbufferflushschedulepoolsize}

Limit on number of tasks in BackgroundBufferFlushSchedulePool

### BackgroundBufferFlushSchedulePoolTask {#backgroundbufferflushschedulepooltask}

Number of active tasks in BackgroundBufferFlushSchedulePool. This pool is used for periodic Buffer flushes

### BackgroundCommonPoolSize {#backgroundcommonpoolsize}

Limit on number of tasks in an associated background pool

### BackgroundCommonPoolTask {#backgroundcommonpooltask}

Number of active tasks in an associated background pool

### BackgroundDistributedSchedulePoolSize {#backgrounddistributedschedulepoolsize}

Limit on number of tasks in BackgroundDistributedSchedulePool

### BackgroundDistributedSchedulePoolTask {#backgrounddistributedschedulepooltask}

Number of active tasks in BackgroundDistributedSchedulePool. This pool is used for distributed sends that is done in background.

### BackgroundFetchesPoolSize {#backgroundfetchespoolsize}

Limit on number of simultaneous fetches in an associated background pool

### BackgroundFetchesPoolTask {#backgroundfetchespooltask}

Number of active fetches in an associated background pool

### BackgroundMergesAndMutationsPoolSize {#backgroundmergesandmutationspoolsize}

Limit on number of active merges and mutations in an associated background pool

### BackgroundMergesAndMutationsPoolTask {#backgroundmergesandmutationspooltask}

Number of active merges and mutations in an associated background pool

### BackgroundMessageBrokerSchedulePoolSize {#backgroundmessagebrokerschedulepoolsize}

Limit on number of tasks in BackgroundProcessingPool for message streaming

### BackgroundMessageBrokerSchedulePoolTask {#backgroundmessagebrokerschedulepooltask}

Number of active tasks in BackgroundProcessingPool for message streaming

### BackgroundMovePoolSize {#backgroundmovepoolsize}

Limit on number of tasks in BackgroundProcessingPool for moves

### BackgroundMovePoolTask {#backgroundmovepooltask}

Number of active tasks in BackgroundProcessingPool for moves

### BackgroundSchedulePoolSize {#backgroundschedulepoolsize}

Limit on number of tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.

### BackgroundSchedulePoolTask {#backgroundschedulepooltask}

Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.

### BackupsIOThreads {#backupsiothreads}

Number of threads in the BackupsIO thread pool.

### BackupsIOThreadsActive {#backupsiothreadsactive}

Number of threads in the BackupsIO thread pool running a task.

### BackupsThreads {#backupsthreads}

Number of threads in the thread pool for BACKUP.

### BackupsThreadsActive {#backupsthreadsactive}

Number of threads in thread pool for BACKUP running a task.

### BrokenDistributedFilesToInsert {#brokendistributedfilestoinsert}

Number of files for asynchronous insertion into Distributed tables that has been marked as broken. This metric will starts from 0 on start. Number of files for every shard is summed.

### CacheDetachedFileSegments {#cachedetachedfilesegments}

Number of existing detached cache file segments

### CacheDictionaryThreads {#cachedictionarythreads}

Number of threads in the CacheDictionary thread pool.

### CacheDictionaryThreadsActive {#cachedictionarythreadsactive}

Number of threads in the CacheDictionary thread pool running a task.

### CacheDictionaryUpdateQueueBatches {#cachedictionaryupdatequeuebatches}

Number of 'batches' (a set of keys) in update queue in CacheDictionaries.

### CacheDictionaryUpdateQueueKeys {#cachedictionaryupdatequeuekeys}

Exact number of keys in update queue in CacheDictionaries.

### CacheFileSegments {#cachefilesegments}

Number of existing cache file segments

### ContextLockWait {#contextlockwait}

Number of threads waiting for lock in Context. This is global lock.

### DDLWorkerThreads {#ddlworkerthreads}

Number of threads in the DDLWorker thread pool for ON CLUSTER queries.

### DDLWorkerThreadsActive {#ddlworkerthreadsactive}

Number of threads in the DDLWORKER thread pool for ON CLUSTER queries running a task.

### DatabaseCatalogThreads {#databasecatalogthreads}

Number of threads in the DatabaseCatalog thread pool.

### DatabaseCatalogThreadsActive {#databasecatalogthreadsactive}

Number of threads in the DatabaseCatalog thread pool running a task.

### DatabaseOnDiskThreads {#databaseondiskthreads}

Number of threads in the DatabaseOnDisk thread pool.

### DatabaseOnDiskThreadsActive {#databaseondiskthreadsactive}

Number of threads in the DatabaseOnDisk thread pool running a task.

### DelayedInserts {#delayedinserts}

Number of INSERT queries that are throttled due to high number of active data parts for partition in a MergeTree table.

### DestroyAggregatesThreads {#destroyaggregatesthreads}

Number of threads in the thread pool for destroy aggregate states.

### DestroyAggregatesThreadsActive {#destroyaggregatesthreadsactive}

Number of threads in the thread pool for destroy aggregate states running a task.

### DictCacheRequests {#dictcacherequests}

Number of requests in fly to data sources of dictionaries of cache type.

### DiskObjectStorageAsyncThreads {#diskobjectstorageasyncthreads}

Number of threads in the async thread pool for DiskObjectStorage.

### DiskObjectStorageAsyncThreadsActive {#diskobjectstorageasyncthreadsactive}

Number of threads in the async thread pool for DiskObjectStorage running a task.

### DiskSpaceReservedForMerge {#diskspacereservedformerge}

Disk space reserved for currently running background merges. It is slightly more than the total size of currently merging parts.

### DistributedFilesToInsert {#distributedfilestoinsert}

Number of pending files to process for asynchronous insertion into Distributed tables. Number of files for every shard is summed.

### DistributedSend {#distributedsend}

Number of connections to remote servers sending data that was INSERTed into Distributed tables. Both synchronous and asynchronous mode.

### EphemeralNode {#ephemeralnode}

Number of ephemeral nodes hold in ZooKeeper.

### FilesystemCacheElements {#filesystemcacheelements}

Filesystem cache elements (file segments)

### FilesystemCacheReadBuffers {#filesystemcachereadbuffers}

Number of active cache buffers

### FilesystemCacheSize {#filesystemcachesize}

Filesystem cache size in bytes

### QueryCacheBytes {#querycachebytes}

Total size of the query cache in bytes.

### QueryCacheEntries {#querycacheentries}

Total number of entries in the query cache.

### UncompressedCacheBytes {#uncompressedcachebytes}

Total size of uncompressed cache in bytes. Uncompressed cache does not usually improve the performance and should be mostly avoided.

### UncompressedCacheCells {#uncompressedcachecells}

### CompiledExpressionCacheBytes {#compiledexpressioncachebytes}

Total bytes used for the cache of JIT-compiled code.

### CompiledExpressionCacheCount {#compiledexpressioncachecount}

Total entries in the cache of JIT-compiled code.

### MMapCacheCells {#mmapcachecells}

The number of files opened with `mmap` (mapped in memory). This is used for queries with the setting `local_filesystem_read_method` set to  `mmap`. The files opened with `mmap` are kept in the cache to avoid costly TLB flushes.

### MarkCacheBytes {#markcachebytes}

Total size of mark cache in bytes

### MarkCacheFiles {#markcachefiles}

Total number of mark files cached in the mark cache

### GlobalThread {#globalthread}

Number of threads in global thread pool.

### GlobalThreadActive {#globalthreadactive}

Number of threads in global thread pool running a task.

### HTTPConnection {#httpconnection}

Number of connections to HTTP server

### HashedDictionaryThreads {#hasheddictionarythreads}

Number of threads in the HashedDictionary thread pool.

### HashedDictionaryThreadsActive {#hasheddictionarythreadsactive}

Number of threads in the HashedDictionary thread pool running a task.

### IOPrefetchThreads {#ioprefetchthreads}

Number of threads in the IO prefetch thread pool.

### IOPrefetchThreadsActive {#ioprefetchthreadsactive}

Number of threads in the IO prefetch thread pool running a task.

### IOThreads {#iothreads}

Number of threads in the IO thread pool.

### IOThreadsActive {#iothreadsactive}

Number of threads in the IO thread pool running a task.

### IOUringInFlightEvents {#iouringinflightevents}

Number of io_uring SQEs in flight

### IOUringPendingEvents {#iouringpendingevents}

Number of io_uring SQEs waiting to be submitted

### IOWriterThreads {#iowriterthreads}

Number of threads in the IO writer thread pool.

### IOWriterThreadsActive {#iowriterthreadsactive}

Number of threads in the IO writer thread pool running a task.

### InterserverConnection {#interserverconnection}

Number of connections from other replicas to fetch parts

### KafkaAssignedPartitions {#kafkaassignedpartitions}

Number of partitions Kafka tables currently assigned to

### KafkaBackgroundReads {#kafkabackgroundreads}

Number of background reads currently working (populating materialized views from Kafka)

### KafkaConsumers {#kafkaconsumers}

Number of active Kafka consumers

### KafkaConsumersInUse {#kafkaconsumersinuse}

Number of consumers which are currently used by direct or background reads

### KafkaConsumersWithAssignment {#kafkaconsumerswithassignment}

Number of active Kafka consumers which have some partitions assigned.

### KafkaLibrdkafkaThreads {#kafkalibrdkafkathreads}

Number of active librdkafka threads

### KafkaProducers {#kafkaproducers}

Number of active Kafka producer created

### KafkaWrites {#kafkawrites}

Number of currently running inserts to Kafka

### KeeperAliveConnections {#keeperaliveconnections}

Number of alive connections

### KeeperOutstandingRequests {#keeperoutstandingrequests}

Number of outstanding requests

### LocalThread {#localthread}

Number of threads in local thread pools. The threads in local thread pools are taken from the global thread pool.

### LocalThreadActive {#localthreadactive}

Number of threads in local thread pools running a task.

### MMappedAllocBytes {#mmappedallocbytes}

Sum bytes of mmapped allocations

### MMappedAllocs {#mmappedallocs}

Total number of mmapped allocations

### MMappedFileBytes {#mmappedfilebytes}

Sum size of mmapped file regions.

### MMappedFiles {#mmappedfiles}

Total number of mmapped files.

### MarksLoaderThreads {#marksloaderthreads}

Number of threads in thread pool for loading marks.

### MarksLoaderThreadsActive {#marksloaderthreadsactive}

Number of threads in the thread pool for loading marks running a task.

### MaxDDLEntryID {#maxddlentryid}

Max processed DDL entry of DDLWorker.

### MaxPushedDDLEntryID {#maxpushedddlentryid}

Max DDL entry of DDLWorker that pushed to zookeeper.

### MemoryTracking {#memorytracking}

Total amount of memory (bytes) allocated by the server.

### Merge {#merge}

Number of executing background merges

### MergeTreeAllRangesAnnouncementsSent {#mergetreeallrangesannouncementssent}

The current number of announcement being sent in flight from the remote server to the initiator server about the set of data parts (for MergeTree tables). Measured on the remote server side.

### MergeTreeBackgroundExecutorThreads {#mergetreebackgroundexecutorthreads}

Number of threads in the MergeTreeBackgroundExecutor thread pool.

### MergeTreeBackgroundExecutorThreadsActive {#mergetreebackgroundexecutorthreadsactive}

Number of threads in the MergeTreeBackgroundExecutor thread pool running a task.

### MergeTreeDataSelectExecutorThreads {#mergetreedataselectexecutorthreads}

Number of threads in the MergeTreeDataSelectExecutor thread pool.

### MergeTreeDataSelectExecutorThreadsActive {#mergetreedataselectexecutorthreadsactive}

Number of threads in the MergeTreeDataSelectExecutor thread pool running a task.

### MergeTreePartsCleanerThreads {#mergetreepartscleanerthreads}

Number of threads in the MergeTree parts cleaner thread pool.

### MergeTreePartsCleanerThreadsActive {#mergetreepartscleanerthreadsactive}

Number of threads in the MergeTree parts cleaner thread pool running a task.

### MergeTreePartsLoaderThreads {#mergetreepartsloaderthreads}

Number of threads in the MergeTree parts loader thread pool.

### MergeTreePartsLoaderThreadsActive {#mergetreepartsloaderthreadsactive}

Number of threads in the MergeTree parts loader thread pool running a task.

### MergeTreeReadTaskRequestsSent {#mergetreereadtaskrequestssent}

The current number of callback requests in flight from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the remote server side.

### Move {#move}

Number of currently executing moves

### MySQLConnection {#mysqlconnection}

Number of client connections using MySQL protocol

### NetworkReceive {#networkreceive}

Number of threads receiving data from network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.

### NetworkSend {#networksend}

Number of threads sending data to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.

### OpenFileForRead {#openfileforread}

Number of files open for reading

### OpenFileForWrite {#openfileforwrite}

Number of files open for writing

### ParallelFormattingOutputFormatThreads {#parallelformattingoutputformatthreads}

Number of threads in the ParallelFormattingOutputFormatThreads thread pool.

### ParallelFormattingOutputFormatThreadsActive {#parallelformattingoutputformatthreadsactive}

Number of threads in the ParallelFormattingOutputFormatThreads thread pool running a task.

### PartMutation {#partmutation}

Number of mutations (ALTER DELETE/UPDATE)

### PartsActive {#partsactive}

Active data part, used by current and upcoming SELECTs.

### PartsCommitted {#partscommitted}

Deprecated. See PartsActive.

### PartsCompact {#partscompact}

Compact parts.

### PartsDeleteOnDestroy {#partsdeleteondestroy}

Part was moved to another disk and should be deleted in own destructor.

### PartsDeleting {#partsdeleting}

Not active data part with identity refcounter, it is deleting right now by a cleaner.

### PartsOutdated {#partsoutdated}

Not active data part, but could be used by only current SELECTs, could be deleted after SELECTs finishes.

### PartsPreActive {#partspreactive}

The part is in data_parts, but not used for SELECTs.

### PartsPreCommitted {#partsprecommitted}

Deprecated. See PartsPreActive.

### PartsTemporary {#partstemporary}

The part is generating now, it is not in data_parts list.

### PartsWide {#partswide}

Wide parts.

### PendingAsyncInsert {#pendingasyncinsert}

Number of asynchronous inserts that are waiting for flush.

### PostgreSQLConnection {#postgresqlconnection}

Number of client connections using PostgreSQL protocol

### Query {#query}

Number of executing queries

### QueryPreempted {#querypreempted}

Number of queries that are stopped and waiting due to 'priority' setting.

### QueryThread {#querythread}

Number of query processing threads

### RWLockActiveReaders {#rwlockactivereaders}

Number of threads holding read lock in a table RWLock.

### RWLockActiveWriters {#rwlockactivewriters}

Number of threads holding write lock in a table RWLock.

### RWLockWaitingReaders {#rwlockwaitingreaders}

Number of threads waiting for read on a table RWLock.

### RWLockWaitingWriters {#rwlockwaitingwriters}

Number of threads waiting for write on a table RWLock.

### Read {#read}

Number of read (read, pread, io_getevents, etc.) syscalls in fly

### ReadTaskRequestsSent {#readtaskrequestssent}

The current number of callback requests in flight from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the remote server side.

### ReadonlyReplica {#readonlyreplica}

Number of Replicated tables that are currently in readonly state due to re-initialization after ZooKeeper session loss or due to startup without ZooKeeper configured.

### RemoteRead {#remoteread}

Number of read with remote reader in fly

### ReplicatedChecks {#replicatedchecks}

Number of data parts checking for consistency

### ReplicatedFetch {#replicatedfetch}

Number of data parts being fetched from replica

### ReplicatedSend {#replicatedsend}

Number of data parts being sent to replicas

### RestartReplicaThreads {#restartreplicathreads}

Number of threads in the RESTART REPLICA thread pool.

### RestartReplicaThreadsActive {#restartreplicathreadsactive}

Number of threads in the RESTART REPLICA thread pool running a task.

### RestoreThreads {#restorethreads}

Number of threads in the thread pool for RESTORE.

### RestoreThreadsActive {#restorethreadsactive}

Number of threads in the thread pool for RESTORE running a task.

### Revision {#revision}

Revision of the server. It is a number incremented for every release or release candidate except patch releases.

### S3Requests {#s3requests}

S3 requests

### SendExternalTables {#sendexternaltables}

Number of connections that are sending data for external tables to remote servers. External tables are used to implement GLOBAL IN and GLOBAL JOIN operators with distributed subqueries.

### SendScalars {#sendscalars}

Number of connections that are sending data for scalars to remote servers.

### StorageBufferBytes {#storagebufferbytes}

Number of bytes in buffers of Buffer tables

### StorageBufferRows {#storagebufferrows}

Number of rows in buffers of Buffer tables

### StorageDistributedThreads {#storagedistributedthreads}

Number of threads in the StorageDistributed thread pool.

### StorageDistributedThreadsActive {#storagedistributedthreadsactive}

Number of threads in the StorageDistributed thread pool running a task.

### StorageHiveThreads {#storagehivethreads}

Number of threads in the StorageHive thread pool.

### StorageHiveThreadsActive {#storagehivethreadsactive}

Number of threads in the StorageHive thread pool running a task.

### StorageS3Threads {#storages3threads}

Number of threads in the StorageS3 thread pool.

### StorageS3ThreadsActive {#storages3threadsactive}

Number of threads in the StorageS3 thread pool running a task.

### SystemReplicasThreads {#systemreplicasthreads}

Number of threads in the system.replicas thread pool.

### SystemReplicasThreadsActive {#systemreplicasthreadsactive}

Number of threads in the system.replicas thread pool running a task.

### TCPConnection {#tcpconnection}

Number of connections to TCP server (clients with native interface), also included server-server distributed query connections

### TablesToDropQueueSize {#tablestodropqueuesize}

Number of dropped tables, that are waiting for background data removal.

### TemporaryFilesForAggregation {#temporaryfilesforaggregation}

Number of temporary files created for external aggregation

### TemporaryFilesForJoin {#temporaryfilesforjoin}

Number of temporary files created for JOIN

### TemporaryFilesForSort {#temporaryfilesforsort}

Number of temporary files created for external sorting

### TemporaryFilesUnknown {#temporaryfilesunknown}

Number of temporary files created without known purpose

### ThreadPoolFSReaderThreads {#threadpoolfsreaderthreads}

Number of threads in the thread pool for local_filesystem_read_method=threadpool.

### ThreadPoolFSReaderThreadsActive {#threadpoolfsreaderthreadsactive}

Number of threads in the thread pool for local_filesystem_read_method=threadpool running a task.

### ThreadPoolRemoteFSReaderThreads {#threadpoolremotefsreaderthreads}

Number of threads in the thread pool for remote_filesystem_read_method=threadpool.

### ThreadPoolRemoteFSReaderThreadsActive {#threadpoolremotefsreaderthreadsactive}

Number of threads in the thread pool for remote_filesystem_read_method=threadpool running a task.

### ThreadsInOvercommitTracker {#threadsinovercommittracker}

Number of waiting threads inside of OvercommitTracker

### TotalTemporaryFiles {#totaltemporaryfiles}

Number of temporary files created

### VersionInteger {#versioninteger}

Version of the server in a single integer number in base-1000. For example, version 11.22.33 is translated to 11022033.

### Write {#write}

Number of write (write, pwrite, io_getevents, etc.) syscalls in fly

### ZooKeeperRequest {#zookeeperrequest}

Number of requests to ZooKeeper in fly.

### ZooKeeperSession {#zookeepersession}

Number of sessions (connections) to ZooKeeper. Should be no more than one, because using more than one connection to ZooKeeper may lead to bugs due to lack of linearizability (stale reads) that ZooKeeper consistency model allows.

### ZooKeeperWatch {#zookeeperwatch}

Number of watches (event subscriptions) in ZooKeeper.

### ConcurrencyControlAcquired {#concurrencycontrolacquired}

Total number of acquired CPU slots.

### ConcurrencyControlSoftLimit {#concurrencycontrolsoftlimit}

Value of soft limit on number of CPU slots.

**See Also**

- [system.asynchronous_metrics](/operations/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/operations/system-tables/events) — Contains a number of events that occurred.
- [system.metric_log](/operations/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
- [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
