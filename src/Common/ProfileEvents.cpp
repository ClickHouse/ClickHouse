#include <Common/LoggingFormatStringHelpers.h>
#include <Common/thread_local_rng.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <Common/TraceSender.h>
#include <Interpreters/Context.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <cfloat>
#include <random>

// clang-format off
/// Available events. Add something here as you wish.
/// If the event is generic (i.e. not server specific)
/// it should be also added to src/Coordination/KeeperConstant.cpp
#define APPLY_FOR_BUILTIN_EVENTS(M) \
    M(Query, "Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.", ValueType::Number) \
    M(SelectQuery, "Same as Query, but only for SELECT queries.", ValueType::Number) \
    M(InsertQuery, "Same as Query, but only for INSERT queries.", ValueType::Number) \
    M(InitialQuery, "Same as Query, but only counts initial queries (see is_initial_query).", ValueType::Number) \
    M(QueriesWithSubqueries, "Count queries with all subqueries", ValueType::Number) \
    M(SelectQueriesWithSubqueries, "Count SELECT queries with all subqueries", ValueType::Number) \
    M(InsertQueriesWithSubqueries, "Count INSERT queries with all subqueries", ValueType::Number) \
    M(SelectQueriesWithPrimaryKeyUsage, "Count SELECT queries which use the primary key to evaluate the WHERE condition", ValueType::Number) \
    M(AsyncInsertQuery, "Same as InsertQuery, but only for asynchronous INSERT queries.", ValueType::Number) \
    M(AsyncInsertBytes, "Data size in bytes of asynchronous INSERT queries.", ValueType::Bytes) \
    M(AsyncInsertRows, "Number of rows inserted by asynchronous INSERT queries.", ValueType::Number) \
    M(AsyncInsertCacheHits, "Number of times a duplicate hash id has been found in asynchronous INSERT hash id cache.", ValueType::Number) \
    M(FailedQuery, "Number of failed queries.", ValueType::Number) \
    M(FailedSelectQuery, "Same as FailedQuery, but only for SELECT queries.", ValueType::Number) \
    M(FailedInsertQuery, "Same as FailedQuery, but only for INSERT queries.", ValueType::Number) \
    M(FailedAsyncInsertQuery, "Number of failed ASYNC INSERT queries.", ValueType::Number) \
    M(QueryTimeMicroseconds, "Total time of all queries.", ValueType::Microseconds) \
    M(SelectQueryTimeMicroseconds, "Total time of SELECT queries.", ValueType::Microseconds) \
    M(InsertQueryTimeMicroseconds, "Total time of INSERT queries.", ValueType::Microseconds) \
    M(OtherQueryTimeMicroseconds, "Total time of queries that are not SELECT or INSERT.", ValueType::Microseconds) \
    M(FileOpen, "Number of files opened.", ValueType::Number) \
    M(Seek, "Number of times the 'lseek' function was called.", ValueType::Number) \
    M(ReadBufferFromFileDescriptorRead, "Number of reads (read/pread) from a file descriptor. Does not include sockets.", ValueType::Number) \
    M(ReadBufferFromFileDescriptorReadFailed, "Number of times the read (read/pread) from a file descriptor have failed.", ValueType::Number) \
    M(ReadBufferFromFileDescriptorReadBytes, "Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.", ValueType::Bytes) \
    M(WriteBufferFromFileDescriptorWrite, "Number of writes (write/pwrite) to a file descriptor. Does not include sockets.", ValueType::Number) \
    M(WriteBufferFromFileDescriptorWriteFailed, "Number of times the write (write/pwrite) to a file descriptor have failed.", ValueType::Number) \
    M(WriteBufferFromFileDescriptorWriteBytes, "Number of bytes written to file descriptors. If the file is compressed, this will show compressed data size.", ValueType::Bytes) \
    M(FileSync, "Number of times the F_FULLFSYNC/fsync/fdatasync function was called for files.", ValueType::Number) \
    M(DirectorySync, "Number of times the F_FULLFSYNC/fsync/fdatasync function was called for directories.", ValueType::Number) \
    M(FileSyncElapsedMicroseconds, "Total time spent waiting for F_FULLFSYNC/fsync/fdatasync syscall for files.", ValueType::Microseconds) \
    M(DirectorySyncElapsedMicroseconds, "Total time spent waiting for F_FULLFSYNC/fsync/fdatasync syscall for directories.", ValueType::Microseconds) \
    M(ReadCompressedBytes, "Number of bytes (the number of bytes before decompression) read from compressed sources (files, network).", ValueType::Bytes) \
    M(CompressedReadBufferBlocks, "Number of compressed blocks (the blocks of data that are compressed independent of each other) read from compressed sources (files, network).", ValueType::Number) \
    M(CompressedReadBufferBytes, "Number of uncompressed bytes (the number of bytes after decompression) read from compressed sources (files, network).", ValueType::Bytes) \
    M(CompressedReadBufferChecksumDoesntMatch, "Number of times the compressed block checksum did not match.", ValueType::Number) \
    M(CompressedReadBufferChecksumDoesntMatchSingleBitMismatch, "Number of times a compressed block checksum mismatch was caused by a single-bit difference.", ValueType::Number) \
    M(CompressedReadBufferChecksumDoesntMatchMicroseconds, "Total time spent detecting bit-flips due to compressed block checksum mismatches.", ValueType::Microseconds) \
    M(UncompressedCacheHits, "Number of times a block of data has been found in the uncompressed cache (and decompression was avoided).", ValueType::Number) \
    M(UncompressedCacheMisses, "Number of times a block of data has not been found in the uncompressed cache (and required decompression).", ValueType::Number) \
    M(UncompressedCacheWeightLost, "Number of bytes evicted from the uncompressed cache.", ValueType::Bytes) \
    M(PageCacheHits, "Number of times a block of data has been found in the userspace page cache.", ValueType::Number) \
    M(PageCacheMisses, "Number of times a block of data has not been found in the userspace page cache.", ValueType::Number) \
    M(PageCacheWeightLost, "Number of bytes evicted from the userspace page cache", ValueType::Bytes) \
    M(PageCacheResized, "Number of times the userspace page cache was auto-resized (typically happens a few times per second, controlled by memory_worker_period_ms).", ValueType::Number) \
    M(PageCacheOvercommitResize, "Number of times the userspace page cache was auto-resized to free memory during a memory allocation.", ValueType::Number) \
    M(PageCacheReadBytes, "Number of bytes read from userspace page cache.", ValueType::Bytes) \
    M(MMappedFileCacheHits, "Number of times a file has been found in the MMap cache (for the 'mmap' read_method), so we didn't have to mmap it again.", ValueType::Number) \
    M(MMappedFileCacheMisses, "Number of times a file has not been found in the MMap cache (for the 'mmap' read_method), so we had to mmap it again.", ValueType::Number) \
    M(OpenedFileCacheHits, "Number of times a file has been found in the opened file cache, so we didn't have to open it again.", ValueType::Number) \
    M(OpenedFileCacheMisses, "Number of times a file has been found in the opened file cache, so we had to open it again.", ValueType::Number) \
    M(OpenedFileCacheMicroseconds, "Amount of time spent executing OpenedFileCache methods.", ValueType::Microseconds) \
    M(AIOWrite, "Number of writes with Linux or FreeBSD AIO interface", ValueType::Number) \
    M(AIOWriteBytes, "Number of bytes written with Linux or FreeBSD AIO interface", ValueType::Bytes) \
    M(AIORead, "Number of reads with Linux or FreeBSD AIO interface", ValueType::Number) \
    M(AIOReadBytes, "Number of bytes read with Linux or FreeBSD AIO interface", ValueType::Bytes) \
    M(IOBufferAllocs, "Number of allocations of IO buffers (for ReadBuffer/WriteBuffer).", ValueType::Number) \
    M(IOBufferAllocBytes, "Number of bytes allocated for IO buffers (for ReadBuffer/WriteBuffer).", ValueType::Bytes) \
    M(ArenaAllocChunks, "Number of chunks allocated for memory Arena (used for GROUP BY and similar operations)", ValueType::Number) \
    M(ArenaAllocBytes, "Number of bytes allocated for memory Arena (used for GROUP BY and similar operations)", ValueType::Bytes) \
    M(FunctionExecute, "Number of SQL ordinary function calls (SQL functions are called on per-block basis, so this number represents the number of blocks).", ValueType::Number) \
    M(TableFunctionExecute, "Number of table function calls.", ValueType::Number) \
    M(DefaultImplementationForNullsRows, "Number of rows processed by default implementation for nulls in function execution", ValueType::Number) \
    M(DefaultImplementationForNullsRowsWithNulls, "Number of rows which contain null values processed by default implementation for nulls in function execution", ValueType::Number) \
    M(MarkCacheHits, "Number of times an entry has been found in the mark cache, so we didn't have to load a mark file.", ValueType::Number) \
    M(MarkCacheMisses, "Number of times an entry has not been found in the mark cache, so we had to load a mark file in memory, which is a costly operation, adding to query latency.", ValueType::Number) \
    M(PrimaryIndexCacheHits, "Number of times an entry has been found in the primary index cache, so we didn't have to load a index file.", ValueType::Number) \
    M(PrimaryIndexCacheMisses, "Number of times an entry has not been found in the primary index cache, so we had to load a index file in memory, which is a costly operation, adding to query latency.", ValueType::Number) \
    M(IcebergMetadataFilesCacheHits, "Number of times iceberg metadata files have been found in the cache.", ValueType::Number) \
    M(IcebergMetadataFilesCacheMisses, "Number of times iceberg metadata files have not been found in the iceberg metadata cache and had to be read from (remote) disk.", ValueType::Number) \
    M(IcebergMetadataFilesCacheWeightLost, "Approximate number of bytes evicted from the iceberg metadata cache.", ValueType::Number) \
    M(VectorSimilarityIndexCacheHits, "Number of times an index granule has been found in the vector index cache.", ValueType::Number) \
    M(VectorSimilarityIndexCacheMisses, "Number of times an index granule has not been found in the vector index cache and had to be read from disk.", ValueType::Number) \
    M(VectorSimilarityIndexCacheWeightLost, "Approximate number of bytes evicted from the vector index cache.", ValueType::Number) \
    M(QueryConditionCacheHits, "Number of times an entry has been found in the query condition cache (and reading of marks can be skipped). Only updated for SELECT queries with SETTING use_query_condition_cache = 1.", ValueType::Number) \
    M(QueryConditionCacheMisses, "Number of times an entry has not been found in the query condition cache (and reading of mark cannot be skipped). Only updated for SELECT queries with SETTING use_query_condition_cache = 1.", ValueType::Number) \
    M(QueryCacheHits, "Number of times a query result has been found in the query cache (and query computation was avoided). Only updated for SELECT queries with SETTING use_query_cache = 1.", ValueType::Number) \
    M(QueryCacheMisses, "Number of times a query result has not been found in the query cache (and required query computation). Only updated for SELECT queries with SETTING use_query_cache = 1.", ValueType::Number) \
    M(CreatedReadBufferOrdinary, "Number of times ordinary read buffer was created for reading data (while choosing among other read methods).", ValueType::Number) \
    M(CreatedReadBufferDirectIO, "Number of times a read buffer with O_DIRECT was created for reading data (while choosing among other read methods).", ValueType::Number) \
    M(CreatedReadBufferDirectIOFailed, "Number of times a read buffer with O_DIRECT was attempted to be created for reading data (while choosing among other read methods), but the OS did not allow it (due to lack of filesystem support or other reasons) and we fallen back to the ordinary reading method.", ValueType::Number) \
    M(CreatedReadBufferMMap, "Number of times a read buffer using 'mmap' was created for reading data (while choosing among other read methods).", ValueType::Number) \
    M(CreatedReadBufferMMapFailed, "Number of times a read buffer with 'mmap' was attempted to be created for reading data (while choosing among other read methods), but the OS did not allow it (due to lack of filesystem support or other reasons) and we fallen back to the ordinary reading method.", ValueType::Number) \
    M(DiskReadElapsedMicroseconds, "Total time spent waiting for read syscall. This include reads from page cache.", ValueType::Microseconds) \
    M(DiskWriteElapsedMicroseconds, "Total time spent waiting for write syscall. This include writes to page cache.", ValueType::Microseconds) \
    M(NetworkReceiveElapsedMicroseconds, "Total time spent waiting for data to receive or receiving data from network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.", ValueType::Microseconds) \
    M(NetworkSendElapsedMicroseconds, "Total time spent waiting for data to send to network or sending data to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.", ValueType::Microseconds) \
    M(NetworkReceiveBytes, "Total number of bytes received from network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.", ValueType::Bytes) \
    M(NetworkSendBytes, "Total number of bytes send to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.", ValueType::Bytes) \
    \
    M(GlobalThreadPoolExpansions, "Counts the total number of times new threads have been added to the global thread pool. This metric indicates the frequency of expansions in the global thread pool to accommodate increased processing demands.", ValueType::Number) \
    M(GlobalThreadPoolShrinks, "Counts the total number of times the global thread pool has shrunk by removing threads. This occurs when the number of idle threads exceeds max_thread_pool_free_size, indicating adjustments in the global thread pool size in response to decreased thread utilization.", ValueType::Number) \
    M(GlobalThreadPoolThreadCreationMicroseconds, "Total time spent waiting for new threads to start.", ValueType::Microseconds) \
    M(GlobalThreadPoolLockWaitMicroseconds, "Total time threads have spent waiting for locks in the global thread pool.", ValueType::Microseconds) \
    M(GlobalThreadPoolJobs, "Counts the number of jobs that have been pushed to the global thread pool.", ValueType::Number) \
    M(GlobalThreadPoolJobWaitTimeMicroseconds, "Measures the elapsed time from when a job is scheduled in the thread pool to when it is picked up for execution by a worker thread. This metric helps identify delays in job processing, indicating the responsiveness of the thread pool to new tasks.", ValueType::Microseconds) \
    M(LocalThreadPoolExpansions, "Counts the total number of times threads have been borrowed from the global thread pool to expand local thread pools.", ValueType::Number) \
    M(LocalThreadPoolShrinks, "Counts the total number of times threads have been returned to the global thread pool from local thread pools.", ValueType::Number) \
    M(LocalThreadPoolThreadCreationMicroseconds, "Total time local thread pools have spent waiting to borrow a thread from the global pool.", ValueType::Microseconds) \
    M(LocalThreadPoolLockWaitMicroseconds, "Total time threads have spent waiting for locks in the local thread pools.", ValueType::Microseconds) \
    M(LocalThreadPoolJobs, "Counts the number of jobs that have been pushed to the local thread pools.", ValueType::Microseconds) \
    M(LocalThreadPoolBusyMicroseconds, "Total time threads have spent executing the actual work.", ValueType::Microseconds) \
    M(LocalThreadPoolJobWaitTimeMicroseconds, "Measures the elapsed time from when a job is scheduled in the thread pool to when it is picked up for execution by a worker thread. This metric helps identify delays in job processing, indicating the responsiveness of the thread pool to new tasks.", ValueType::Microseconds) \
    \
    M(DiskS3GetRequestThrottlerCount, "Number of DiskS3 GET and SELECT requests passed through throttler.", ValueType::Number) \
    M(DiskS3GetRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform DiskS3 GET and SELECT request throttling.", ValueType::Microseconds) \
    M(DiskS3PutRequestThrottlerCount, "Number of DiskS3 PUT, COPY, POST and LIST requests passed through throttler.", ValueType::Number) \
    M(DiskS3PutRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform DiskS3 PUT, COPY, POST and LIST request throttling.", ValueType::Microseconds) \
    M(S3GetRequestThrottlerCount, "Number of S3 GET and SELECT requests passed through throttler.", ValueType::Number) \
    M(S3GetRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform S3 GET and SELECT request throttling.", ValueType::Microseconds) \
    M(S3PutRequestThrottlerCount, "Number of S3 PUT, COPY, POST and LIST requests passed through throttler.", ValueType::Number) \
    M(S3PutRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform S3 PUT, COPY, POST and LIST request throttling.", ValueType::Microseconds) \
    M(RemoteReadThrottlerBytes, "Bytes passed through 'max_remote_read_network_bandwidth_for_server'/'max_remote_read_network_bandwidth' throttler.", ValueType::Bytes) \
    M(RemoteReadThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_remote_read_network_bandwidth_for_server'/'max_remote_read_network_bandwidth' throttling.", ValueType::Microseconds) \
    M(RemoteWriteThrottlerBytes, "Bytes passed through 'max_remote_write_network_bandwidth_for_server'/'max_remote_write_network_bandwidth' throttler.", ValueType::Bytes) \
    M(RemoteWriteThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_remote_write_network_bandwidth_for_server'/'max_remote_write_network_bandwidth' throttling.", ValueType::Microseconds) \
    M(LocalReadThrottlerBytes, "Bytes passed through 'max_local_read_bandwidth_for_server'/'max_local_read_bandwidth' throttler.", ValueType::Bytes) \
    M(LocalReadThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_local_read_bandwidth_for_server'/'max_local_read_bandwidth' throttling.", ValueType::Microseconds) \
    M(LocalWriteThrottlerBytes, "Bytes passed through 'max_local_write_bandwidth_for_server'/'max_local_write_bandwidth' throttler.", ValueType::Bytes) \
    M(LocalWriteThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_local_write_bandwidth_for_server'/'max_local_write_bandwidth' throttling.", ValueType::Microseconds) \
    M(ThrottlerSleepMicroseconds, "Total time a query was sleeping to conform all throttling settings.", ValueType::Microseconds) \
    M(ReadTasksWithAppliedPatches, "Total number of read tasks for which there was any patch part applied", ValueType::Number) \
    M(PatchesAppliedInAllReadTasks, "Total number of applied patch parts among all read tasks", ValueType::Number) \
    M(PatchesMergeAppliedInAllReadTasks, "Total number of applied patch parts with Merge mode among all read tasks", ValueType::Number) \
    M(PatchesJoinAppliedInAllReadTasks, "Total number of applied patch parts with Join mode among all read tasks", ValueType::Number) \
    M(ApplyPatchesMicroseconds, "Total time spent applying patch parts to blocks", ValueType::Number) \
    M(ReadPatchesMicroseconds, "Total time spent reading patch parts", ValueType::Number) \
    M(BuildPatchesMergeMicroseconds, "Total time spent building indexes for applying patch parts with Merge mode", ValueType::Number) \
    M(BuildPatchesJoinMicroseconds, "Total time spent building indexes and hash tables for applying patch parts with Join mode", ValueType::Number) \
    M(AnalyzePatchRangesMicroseconds, "Total time spent analyzing index of patch parts", ValueType::Number) \
    M(ReadTasksWithAppliedMutationsOnFly, "Total number of read tasks for which there was any mutation applied on fly", ValueType::Number) \
    M(MutationsAppliedOnFlyInAllReadTasks, "Total number of applied mutations on-fly among all read tasks", ValueType::Number) \
    \
    M(SchedulerIOReadRequests, "Resource requests passed through scheduler for IO reads.", ValueType::Number) \
    M(SchedulerIOReadBytes, "Bytes passed through scheduler for IO reads.", ValueType::Bytes) \
    M(SchedulerIOReadWaitMicroseconds, "Total time a query was waiting on resource requests for IO reads.", ValueType::Microseconds) \
    M(SchedulerIOWriteRequests, "Resource requests passed through scheduler for IO writes.", ValueType::Number) \
    M(SchedulerIOWriteBytes, "Bytes passed through scheduler for IO writes.", ValueType::Bytes) \
    M(SchedulerIOWriteWaitMicroseconds, "Total time a query was waiting on resource requests for IO writes.", ValueType::Microseconds) \
    \
    M(QueryMaskingRulesMatch, "Number of times query masking rules was successfully matched.", ValueType::Number) \
    \
    M(ReplicatedPartFetches, "Number of times a data part was downloaded from replica of a ReplicatedMergeTree table.", ValueType::Number) \
    M(ReplicatedPartFailedFetches, "Number of times a data part was failed to download from replica of a ReplicatedMergeTree table.", ValueType::Number) \
    M(ObsoleteReplicatedParts, "Number of times a data part was covered by another data part that has been fetched from a replica (so, we have marked a covered data part as obsolete and no longer needed).", ValueType::Number) \
    M(ReplicatedPartMerges, "Number of times data parts of ReplicatedMergeTree tables were successfully merged.", ValueType::Number) \
    M(ReplicatedPartFetchesOfMerged, "Number of times we prefer to download already merged part from replica of ReplicatedMergeTree table instead of performing a merge ourself (usually we prefer doing a merge ourself to save network traffic). This happens when we have not all source parts to perform a merge or when the data part is old enough.", ValueType::Number) \
    M(ReplicatedPartMutations, "Number of times data parts of ReplicatedMergeTree tables were successfully mutated.", ValueType::Number) \
    M(ReplicatedPartChecks, "Number of times we had to perform advanced search for a data part on replicas or to clarify the need of an existing data part.", ValueType::Number) \
    M(ReplicatedPartChecksFailed, "Number of times the advanced search for a data part on replicas did not give result or when unexpected part has been found and moved away.", ValueType::Number) \
    M(ReplicatedDataLoss, "Number of times a data part that we wanted doesn't exist on any replica (even on replicas that are offline right now). That data parts are definitely lost. This is normal due to asynchronous replication (if quorum inserts were not enabled), when the replica on which the data part was written was failed and when it became online after fail it doesn't contain that data part.", ValueType::Number) \
    M(ReplicatedCoveredPartsInZooKeeperOnStart, "For debugging purposes. Number of parts in ZooKeeper that have a covering part, but doesn't exist on disk. Checked on server start.", ValueType::Number) \
    \
    M(InsertedRows, "Number of rows INSERTed to all tables.", ValueType::Number) \
    M(InsertedBytes, "Number of bytes (uncompressed; for columns as they stored in memory) INSERTed to all tables.", ValueType::Bytes) \
    M(DelayedInserts, "Number of times the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.", ValueType::Number) \
    M(RejectedInserts, "Number of times the INSERT of a block to a MergeTree table was rejected with 'Too many parts' exception due to high number of active data parts for partition.", ValueType::Number) \
    M(DelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.", ValueType::Milliseconds) \
    M(DelayedMutations, "Number of times the mutation of a MergeTree table was throttled due to high number of unfinished mutations for table.", ValueType::Number) \
    M(RejectedMutations, "Number of times the mutation of a MergeTree table was rejected with 'Too many mutations' exception due to high number of unfinished mutations for table.", ValueType::Number) \
    M(DelayedMutationsMilliseconds, "Total number of milliseconds spent while the mutation of a MergeTree table was throttled due to high number of unfinished mutations for table.", ValueType::Milliseconds) \
    M(DistributedDelayedInserts, "Number of times the INSERT of a block to a Distributed table was throttled due to high number of pending bytes.", ValueType::Number) \
    M(DistributedRejectedInserts, "Number of times the INSERT of a block to a Distributed table was rejected with 'Too many bytes' exception due to high number of pending bytes.", ValueType::Number) \
    M(DistributedDelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a Distributed table was throttled due to high number of pending bytes.", ValueType::Milliseconds) \
    M(DuplicatedInsertedBlocks, "Number of times the INSERTed block to a ReplicatedMergeTree table was deduplicated.", ValueType::Number) \
    \
    M(ZooKeeperInit, "Number of times connection with ZooKeeper has been established.", ValueType::Number) \
    M(ZooKeeperTransactions, "Number of ZooKeeper operations, which include both read and write operations as well as multi-transactions.", ValueType::Number) \
    M(ZooKeeperList, "Number of 'list' (getChildren) requests to ZooKeeper.", ValueType::Number) \
    M(ZooKeeperCreate, "Number of 'create' requests to ZooKeeper.", ValueType::Number) \
    M(ZooKeeperRemove, "Number of 'remove' requests to ZooKeeper.", ValueType::Number) \
    M(ZooKeeperExists, "Number of 'exists' requests to ZooKeeper.", ValueType::Number) \
    M(ZooKeeperGet, "Number of 'get' requests to ZooKeeper.", ValueType::Number) \
    M(ZooKeeperSet, "Number of 'set' requests to ZooKeeper.", ValueType::Number) \
    M(ZooKeeperMulti, "Number of 'multi' requests to ZooKeeper (compound transactions).", ValueType::Number) \
    M(ZooKeeperMultiRead, "Number of read 'multi' requests to ZooKeeper (compound transactions).", ValueType::Number) \
    M(ZooKeeperMultiWrite, "Number of write 'multi' requests to ZooKeeper (compound transactions).", ValueType::Number) \
    M(ZooKeeperCheck, "Number of 'check' requests to ZooKeeper. Usually they don't make sense in isolation, only as part of a complex transaction.", ValueType::Number) \
    M(ZooKeeperSync, "Number of 'sync' requests to ZooKeeper. These requests are rarely needed or usable.", ValueType::Number) \
    M(ZooKeeperReconfig, "Number of 'reconfig' requests to ZooKeeper.", ValueType::Number) \
    M(ZooKeeperClose, "Number of times connection with ZooKeeper has been closed voluntary.", ValueType::Number) \
    M(ZooKeeperWatchResponse, "Number of times watch notification has been received from ZooKeeper.", ValueType::Number) \
    M(ZooKeeperUserExceptions, "Number of exceptions while working with ZooKeeper related to the data (no node, bad version or similar).", ValueType::Number) \
    M(ZooKeeperHardwareExceptions, "Number of exceptions while working with ZooKeeper related to network (connection loss or similar).", ValueType::Number) \
    M(ZooKeeperOtherExceptions, "Number of exceptions while working with ZooKeeper other than ZooKeeperUserExceptions and ZooKeeperHardwareExceptions.", ValueType::Number) \
    M(ZooKeeperWaitMicroseconds, "Number of microseconds spent waiting for responses from ZooKeeper after creating a request, summed across all the requesting threads.", ValueType::Microseconds) \
    M(ZooKeeperBytesSent, "Number of bytes send over network while communicating with ZooKeeper.", ValueType::Bytes) \
    M(ZooKeeperBytesReceived, "Number of bytes received over network while communicating with ZooKeeper.", ValueType::Bytes) \
    \
    M(DistributedConnectionTries, "Total count of distributed connection attempts.", ValueType::Number) \
    M(DistributedConnectionUsable, "Total count of successful distributed connections to a usable server (with required table, but maybe stale).", ValueType::Number) \
    M(DistributedConnectionFailTry, "Total count when distributed connection fails with retry.", ValueType::Number) \
    M(DistributedConnectionMissingTable, "Number of times we rejected a replica from a distributed query, because it did not contain a table needed for the query.", ValueType::Number) \
    M(DistributedConnectionStaleReplica, "Number of times we rejected a replica from a distributed query, because some table needed for a query had replication lag higher than the configured threshold.", ValueType::Number) \
    M(DistributedConnectionSkipReadOnlyReplica, "Number of replicas skipped during INSERT into Distributed table due to replicas being read-only", ValueType::Number) \
    M(DistributedConnectionFailAtAll, "Total count when distributed connection fails after all retries finished.", ValueType::Number) \
    \
    M(HedgedRequestsChangeReplica, "Total count when timeout for changing replica expired in hedged requests.", ValueType::Number) \
    M(SuspendSendingQueryToShard, "Total count when sending query to shard was suspended when async_query_sending_for_remote is enabled.", ValueType::Number) \
    \
    M(CompileFunction, "Number of times a compilation of generated LLVM code (to create fused function for complex expressions) was initiated.", ValueType::Number) \
    M(CompiledFunctionExecute, "Number of times a compiled function was executed.", ValueType::Number) \
    M(CompileExpressionsMicroseconds, "Total time spent for compilation of expressions to LLVM code.", ValueType::Microseconds) \
    M(CompileExpressionsBytes, "Number of bytes used for expressions compilation.", ValueType::Bytes) \
    \
    M(ExecuteShellCommand, "Number of shell command executions.", ValueType::Number) \
    \
    M(ExternalProcessingCompressedBytesTotal, "Number of compressed bytes written by external processing (sorting/aggragating/joining)", ValueType::Bytes) \
    M(ExternalProcessingUncompressedBytesTotal, "Amount of data (uncompressed, before compression) written by external processing (sorting/aggragating/joining)", ValueType::Bytes) \
    M(ExternalProcessingFilesTotal, "Number of files used by external processing (sorting/aggragating/joining)", ValueType::Number) \
    M(ExternalSortWritePart, "Number of times a temporary file was written to disk for sorting in external memory.", ValueType::Number) \
    M(ExternalSortMerge, "Number of times temporary files were merged for sorting in external memory.", ValueType::Number) \
    M(ExternalSortCompressedBytes, "Number of compressed bytes written for sorting in external memory.", ValueType::Bytes) \
    M(ExternalSortUncompressedBytes, "Amount of data (uncompressed, before compression) written for sorting in external memory.", ValueType::Bytes) \
    M(ExternalAggregationWritePart, "Number of times a temporary file was written to disk for aggregation in external memory.", ValueType::Number) \
    M(ExternalAggregationMerge, "Number of times temporary files were merged for aggregation in external memory.", ValueType::Number) \
    M(ExternalAggregationCompressedBytes, "Number of bytes written to disk for aggregation in external memory.", ValueType::Bytes) \
    M(ExternalAggregationUncompressedBytes, "Amount of data (uncompressed, before compression) written to disk for aggregation in external memory.", ValueType::Bytes) \
    M(ExternalJoinWritePart, "Number of times a temporary file was written to disk for JOIN in external memory.", ValueType::Number) \
    M(ExternalJoinMerge, "Number of times temporary files were merged for JOIN in external memory.", ValueType::Number) \
    M(ExternalJoinCompressedBytes, "Number of compressed bytes written for JOIN in external memory.", ValueType::Bytes) \
    M(ExternalJoinUncompressedBytes, "Amount of data (uncompressed, before compression) written for JOIN in external memory.", ValueType::Bytes) \
    \
    M(IcebergPartitionPrunedFiles, "Number of skipped files during Iceberg partition pruning", ValueType::Number) \
    M(IcebergTrivialCountOptimizationApplied, "Trivial count optimization applied while reading from Iceberg", ValueType::Number) \
    M(IcebergVersionHintUsed, "Number of times version-hint.text has been used.", ValueType::Number) \
    M(IcebergMinMaxIndexPrunedFiles, "Number of skipped files by using MinMax index in Iceberg", ValueType::Number) \
    M(JoinBuildTableRowCount, "Total number of rows in the build table for a JOIN operation.", ValueType::Number) \
    M(JoinProbeTableRowCount, "Total number of rows in the probe table for a JOIN operation.", ValueType::Number) \
    M(JoinResultRowCount, "Total number of rows in the result of a JOIN operation.", ValueType::Number) \
    \
    M(DeltaLakePartitionPrunedFiles, "Number of skipped files during DeltaLake partition pruning", ValueType::Number) \
    \
    M(SlowRead, "Number of reads from a file that were slow. This indicate system overload. Thresholds are controlled by read_backoff_* settings.", ValueType::Number) \
    M(ReadBackoff, "Number of times the number of query processing threads was lowered due to slow reads.", ValueType::Number) \
    \
    M(ReplicaPartialShutdown, "How many times Replicated table has to deinitialize its state due to session expiration in ZooKeeper. The state is reinitialized every time when ZooKeeper is available again.", ValueType::Number) \
    \
    M(SelectedParts, "Number of data parts selected to read from a MergeTree table.", ValueType::Number) \
    M(SelectedPartsTotal, "Number of total data parts before selecting which ones to read from a MergeTree table.", ValueType::Number) \
    M(SelectedRanges, "Number of (non-adjacent) ranges in all data parts selected to read from a MergeTree table.", ValueType::Number) \
    M(SelectedMarks, "Number of marks (index granules) selected to read from a MergeTree table.", ValueType::Number) \
    M(SelectedMarksTotal, "Number of total marks (index granules) before selecting which ones to read from a MergeTree table.", ValueType::Number) \
    M(SelectedRows, "Number of rows SELECTed from all tables.", ValueType::Number) \
    M(SelectedBytes, "Number of bytes (uncompressed; for columns as they stored in memory) SELECTed from all tables.", ValueType::Bytes) \
    M(RowsReadByMainReader, "Number of rows read from MergeTree tables by the main reader (after PREWHERE step).", ValueType::Number) \
    M(RowsReadByPrewhereReaders, "Number of rows read from MergeTree tables (in total) by prewhere readers.", ValueType::Number) \
    M(LoadedDataParts, "Number of data parts loaded by MergeTree tables during initialization.", ValueType::Number) \
    M(LoadedDataPartsMicroseconds, "Microseconds spent by MergeTree tables for loading data parts during initialization.", ValueType::Microseconds) \
    M(FilteringMarksWithPrimaryKeyMicroseconds, "Time spent filtering parts by PK.", ValueType::Microseconds) \
    M(FilteringMarksWithSecondaryKeysMicroseconds, "Time spent filtering parts by skip indexes.", ValueType::Microseconds) \
    \
    M(WaitMarksLoadMicroseconds, "Time spent loading marks", ValueType::Microseconds) \
    M(BackgroundLoadingMarksTasks, "Number of background tasks for loading marks", ValueType::Number) \
    M(LoadingMarksTasksCanceled, "Number of times background tasks for loading marks were canceled", ValueType::Number) \
    M(LoadedMarksFiles, "Number of mark files loaded.", ValueType::Number) \
    M(LoadedMarksCount, "Number of marks loaded (total across columns).", ValueType::Number) \
    M(LoadedMarksMemoryBytes, "Size of in-memory representations of loaded marks.", ValueType::Bytes) \
    M(MarkCacheEvictedBytes, "Number of bytes evicted from the mark cache.", ValueType::Bytes) \
    M(MarkCacheEvictedMarks, "Number of marks evicted from the mark cache.", ValueType::Number) \
    M(MarkCacheEvictedFiles, "Number of mark files evicted from the mark cache.", ValueType::Number) \
    M(LoadedPrimaryIndexFiles, "Number of primary index files loaded.", ValueType::Number) \
    M(LoadedPrimaryIndexRows, "Number of rows of primary key loaded.", ValueType::Number) \
    M(LoadedPrimaryIndexBytes, "Number of rows of primary key loaded.", ValueType::Bytes) \
    \
    M(Merge, "Number of launched background merges.", ValueType::Number) \
    M(MergeSourceParts, "Number of source parts scheduled for merges.", ValueType::Number) \
    M(MergedRows, "Rows read for background merges. This is the number of rows before merge.", ValueType::Number) \
    M(MergedColumns, "Number of columns merged during the horizontal stage of merges.", ValueType::Number) \
    M(GatheredColumns, "Number of columns gathered during the vertical stage of merges.", ValueType::Number) \
    M(MergedUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) that was read for background merges. This is the number before merge.", ValueType::Bytes) \
    M(MergeTotalMilliseconds, "Total time spent for background merges", ValueType::Milliseconds) \
    M(MergeExecuteMilliseconds, "Total busy time spent for execution of background merges", ValueType::Milliseconds) \
    M(MergeHorizontalStageTotalMilliseconds, "Total time spent for horizontal stage of background merges", ValueType::Milliseconds) \
    M(MergeHorizontalStageExecuteMilliseconds, "Total busy time spent for execution of horizontal stage of background merges", ValueType::Milliseconds) \
    M(MergeVerticalStageTotalMilliseconds, "Total time spent for vertical stage of background merges", ValueType::Milliseconds) \
    M(MergeVerticalStageExecuteMilliseconds, "Total busy time spent for execution of vertical stage of background merges", ValueType::Milliseconds) \
    M(MergeProjectionStageTotalMilliseconds, "Total time spent for projection stage of background merges", ValueType::Milliseconds) \
    M(MergeProjectionStageExecuteMilliseconds, "Total busy time spent for execution of projection stage of background merges", ValueType::Milliseconds) \
    M(MergePrewarmStageTotalMilliseconds, "Total time spent for prewarm stage of background merges", ValueType::Milliseconds) \
    M(MergePrewarmStageExecuteMilliseconds, "Total busy time spent for execution of prewarm stage of background merges", ValueType::Milliseconds) \
    \
    M(MergingSortedMilliseconds, "Total time spent while merging sorted columns", ValueType::Milliseconds) \
    M(AggregatingSortedMilliseconds, "Total time spent while aggregating sorted columns", ValueType::Milliseconds) \
    M(CoalescingSortedMilliseconds, "Total time spent while coalescing sorted columns", ValueType::Milliseconds) \
    M(CollapsingSortedMilliseconds, "Total time spent while collapsing sorted columns", ValueType::Milliseconds) \
    M(ReplacingSortedMilliseconds, "Total time spent while replacing sorted columns", ValueType::Milliseconds) \
    M(SummingSortedMilliseconds, "Total time spent while summing sorted columns", ValueType::Milliseconds) \
    M(VersionedCollapsingSortedMilliseconds, "Total time spent while version collapsing sorted columns", ValueType::Milliseconds) \
    M(GatheringColumnMilliseconds, "Total time spent while gathering columns for vertical merge", ValueType::Milliseconds) \
    \
    M(MutationTotalParts, "Number of total parts for which mutations tried to be applied", ValueType::Number) \
    M(MutationUntouchedParts, "Number of total parts for which mutations tried to be applied but which was completely skipped according to predicate", ValueType::Number) \
    M(MutationCreatedEmptyParts, "Number of total parts which were replaced to empty parts instead of running mutation", ValueType::Number) \
    M(MutatedRows, "Rows read for mutations. This is the number of rows before mutation", ValueType::Number) \
    M(MutatedUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) that was read for mutations. This is the number before mutation.", ValueType::Bytes) \
    M(MutationTotalMilliseconds, "Total time spent for mutations.", ValueType::Milliseconds) \
    M(MutationExecuteMilliseconds, "Total busy time spent for execution of mutations.", ValueType::Milliseconds) \
    M(MutationAllPartColumns, "Number of times when task to mutate all columns in part was created", ValueType::Number) \
    M(MutationSomePartColumns, "Number of times when task to mutate some columns in part was created", ValueType::Number) \
    M(MutateTaskProjectionsCalculationMicroseconds, "Time spent calculating projections in mutations", ValueType::Microseconds) \
    \
    M(MergeTreeDataWriterRows, "Number of rows INSERTed to MergeTree tables.", ValueType::Number) \
    M(MergeTreeDataWriterUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables.", ValueType::Bytes) \
    M(MergeTreeDataWriterCompressedBytes, "Bytes written to filesystem for data INSERTed to MergeTree tables.", ValueType::Bytes) \
    M(MergeTreeDataWriterBlocks, "Number of blocks INSERTed to MergeTree tables. Each block forms a data part of level zero.", ValueType::Number) \
    M(MergeTreeDataWriterBlocksAlreadySorted, "Number of blocks INSERTed to MergeTree tables that appeared to be already sorted.", ValueType::Number) \
    \
    M(MergeTreeDataWriterSkipIndicesCalculationMicroseconds, "Time spent calculating skip indices", ValueType::Microseconds) \
    M(MergeTreeDataWriterStatisticsCalculationMicroseconds, "Time spent calculating statistics", ValueType::Microseconds) \
    M(MergeTreeDataWriterSortingBlocksMicroseconds, "Time spent sorting blocks", ValueType::Microseconds) \
    M(MergeTreeDataWriterMergingBlocksMicroseconds, "Time spent merging input blocks (for special MergeTree engines)", ValueType::Microseconds) \
    M(MergeTreeDataWriterProjectionsCalculationMicroseconds, "Time spent calculating projections", ValueType::Microseconds) \
    M(MergeTreeDataProjectionWriterSortingBlocksMicroseconds, "Time spent sorting blocks (for projection it might be a key different from table's sorting key)", ValueType::Microseconds) \
    M(MergeTreeDataProjectionWriterMergingBlocksMicroseconds, "Time spent merging blocks", ValueType::Microseconds) \
    \
    M(InsertedWideParts, "Number of parts inserted in Wide format.", ValueType::Number) \
    M(InsertedCompactParts, "Number of parts inserted in Compact format.", ValueType::Number) \
    M(MergedIntoWideParts, "Number of parts merged into Wide format.", ValueType::Number) \
    M(MergedIntoCompactParts, "Number of parts merged into Compact format.", ValueType::Number) \
    \
    M(MergeTreeDataProjectionWriterRows, "Number of rows INSERTed to MergeTree tables projection.", ValueType::Number) \
    M(MergeTreeDataProjectionWriterUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables projection.", ValueType::Bytes) \
    M(MergeTreeDataProjectionWriterCompressedBytes, "Bytes written to filesystem for data INSERTed to MergeTree tables projection.", ValueType::Bytes) \
    M(MergeTreeDataProjectionWriterBlocks, "Number of blocks INSERTed to MergeTree tables projection. Each block forms a data part of level zero.", ValueType::Number) \
    M(MergeTreeDataProjectionWriterBlocksAlreadySorted, "Number of blocks INSERTed to MergeTree tables projection that appeared to be already sorted.", ValueType::Number) \
    \
    M(CannotRemoveEphemeralNode, "Number of times an error happened while trying to remove ephemeral node. This is not an issue, because our implementation of ZooKeeper library guarantee that the session will expire and the node will be removed.", ValueType::Number) \
    \
    M(RegexpWithMultipleNeedlesCreated, "Regular expressions with multiple needles (VectorScan library) compiled.", ValueType::Number) \
    M(RegexpWithMultipleNeedlesGlobalCacheHit, "Number of times we fetched compiled regular expression with multiple needles (VectorScan library) from the global cache.", ValueType::Number) \
    M(RegexpWithMultipleNeedlesGlobalCacheMiss, "Number of times we failed to fetch compiled regular expression with multiple needles (VectorScan library) from the global cache.", ValueType::Number) \
    M(RegexpLocalCacheHit, "Number of times we fetched compiled regular expression from a local cache.", ValueType::Number) \
    M(RegexpLocalCacheMiss, "Number of times we failed to fetch compiled regular expression from a local cache.", ValueType::Number) \
    \
    M(ContextLock, "Number of times the lock of Context was acquired or tried to acquire. This is global lock.", ValueType::Number) \
    M(ContextLockWaitMicroseconds, "Context lock wait time in microseconds", ValueType::Microseconds) \
    \
    M(StorageBufferFlush, "Number of times a buffer in a 'Buffer' table was flushed.", ValueType::Number) \
    M(StorageBufferErrorOnFlush, "Number of times a buffer in the 'Buffer' table has not been able to flush due to error writing in the destination table.", ValueType::Number) \
    M(StorageBufferPassedAllMinThresholds, "Number of times a criteria on min thresholds has been reached to flush a buffer in a 'Buffer' table.", ValueType::Number) \
    M(StorageBufferPassedTimeMaxThreshold, "Number of times a criteria on max time threshold has been reached to flush a buffer in a 'Buffer' table.", ValueType::Number) \
    M(StorageBufferPassedRowsMaxThreshold, "Number of times a criteria on max rows threshold has been reached to flush a buffer in a 'Buffer' table.", ValueType::Number) \
    M(StorageBufferPassedBytesMaxThreshold, "Number of times a criteria on max bytes threshold has been reached to flush a buffer in a 'Buffer' table.", ValueType::Number) \
    M(StorageBufferPassedTimeFlushThreshold, "Number of times background-only flush threshold on time has been reached to flush a buffer in a 'Buffer' table. This is expert-only metric. If you read this and you are not an expert, stop reading.", ValueType::Number) \
    M(StorageBufferPassedRowsFlushThreshold, "Number of times background-only flush threshold on rows has been reached to flush a buffer in a 'Buffer' table. This is expert-only metric. If you read this and you are not an expert, stop reading.", ValueType::Number) \
    M(StorageBufferPassedBytesFlushThreshold, "Number of times background-only flush threshold on bytes has been reached to flush a buffer in a 'Buffer' table. This is expert-only metric. If you read this and you are not an expert, stop reading.", ValueType::Number) \
    M(StorageBufferLayerLockReadersWaitMilliseconds, "Time for waiting for Buffer layer during reading.", ValueType::Milliseconds) \
    M(StorageBufferLayerLockWritersWaitMilliseconds, "Time for waiting free Buffer layer to write to (can be used to tune Buffer layers).", ValueType::Milliseconds) \
    \
    M(SystemLogErrorOnFlush, "Number of times any of the system logs have failed to flush to the corresponding system table. Attempts to flush are repeated.", ValueType::Number) \
    \
    M(DictCacheKeysRequested, "Number of keys requested from the data source for the dictionaries of 'cache' types.", ValueType::Number) \
    M(DictCacheKeysRequestedMiss, "Number of keys requested from the data source for dictionaries of 'cache' types but not found in the data source.", ValueType::Number) \
    M(DictCacheKeysRequestedFound, "Number of keys requested from the data source for dictionaries of 'cache' types and found in the data source.", ValueType::Number) \
    M(DictCacheKeysExpired, "Number of keys looked up in the dictionaries of 'cache' types and found in the cache but they were obsolete.", ValueType::Number) \
    M(DictCacheKeysNotFound, "Number of keys looked up in the dictionaries of 'cache' types and not found.", ValueType::Number) \
    M(DictCacheKeysHit, "Number of keys looked up in the dictionaries of 'cache' types and found in the cache.", ValueType::Number) \
    M(DictCacheRequestTimeNs, "Number of nanoseconds spend in querying the external data sources for the dictionaries of 'cache' types.", ValueType::Nanoseconds) \
    M(DictCacheRequests, "Number of bulk requests to the external data sources for the dictionaries of 'cache' types.", ValueType::Number) \
    M(DictCacheLockWriteNs, "Number of nanoseconds spend in waiting for write lock to update the data for the dictionaries of 'cache' types.", ValueType::Nanoseconds) \
    M(DictCacheLockReadNs, "Number of nanoseconds spend in waiting for read lock to lookup the data for the dictionaries of 'cache' types.", ValueType::Nanoseconds) \
    \
    M(DistributedSyncInsertionTimeoutExceeded, "A timeout has exceeded while waiting for shards during synchronous insertion into a Distributed table (with 'distributed_foreground_insert' = 1)", ValueType::Number) \
    M(DistributedAsyncInsertionFailures, "Number of failures for asynchronous insertion into a Distributed table (with 'distributed_foreground_insert' = 0)", ValueType::Number) \
    M(DataAfterMergeDiffersFromReplica, R"(
Number of times data after merge is not byte-identical to the data on another replicas. There could be several reasons:
1. Using newer version of compression library after server update.
2. Using another compression method.
3. Non-deterministic compression algorithm (highly unlikely).
4. Non-deterministic merge algorithm due to logical error in code.
5. Data corruption in memory due to bug in code.
6. Data corruption in memory due to hardware issue.
7. Manual modification of source data after server startup.
8. Manual modification of checksums stored in ZooKeeper.
9. Part format related settings like 'enable_mixed_granularity_parts' are different on different replicas.
The server successfully detected this situation and will download merged part from the replica to force the byte-identical result.
)", ValueType::Number) \
    M(DataAfterMutationDiffersFromReplica, "Number of times data after mutation is not byte-identical to the data on other replicas. In addition to the reasons described in 'DataAfterMergeDiffersFromReplica', it is also possible due to non-deterministic mutation.", ValueType::Number) \
    M(PolygonsAddedToPool, "A polygon has been added to the cache (pool) for the 'pointInPolygon' function.", ValueType::Number) \
    M(PolygonsInPoolAllocatedBytes, "The number of bytes for polygons added to the cache (pool) for the 'pointInPolygon' function.", ValueType::Bytes) \
    \
    M(USearchAddCount, "Number of vectors added to usearch indexes.", ValueType::Number) \
    M(USearchAddVisitedMembers, "Number of nodes visited when adding vectors to usearch indexes.", ValueType::Number) \
    M(USearchAddComputedDistances, "Number of times distance was computed when adding vectors to usearch indexes.", ValueType::Number) \
    M(USearchSearchCount, "Number of search operations performed in usearch indexes.", ValueType::Number) \
    M(USearchSearchVisitedMembers, "Number of nodes visited when searching in usearch indexes.", ValueType::Number) \
    M(USearchSearchComputedDistances, "Number of times distance was computed when searching usearch indexes.", ValueType::Number) \
    \
    M(RWLockAcquiredReadLocks, "Number of times a read lock was acquired (in a heavy RWLock).", ValueType::Number) \
    M(RWLockAcquiredWriteLocks, "Number of times a write lock was acquired (in a heavy RWLock).", ValueType::Number) \
    M(RWLockReadersWaitMilliseconds, "Total time spent waiting for a read lock to be acquired (in a heavy RWLock).", ValueType::Milliseconds) \
    M(RWLockWritersWaitMilliseconds, "Total time spent waiting for a write lock to be acquired (in a heavy RWLock).", ValueType::Milliseconds) \
    M(DNSError, "Total count of errors in DNS resolution", ValueType::Number) \
    M(PartsLockHoldMicroseconds, "Total time spent holding data parts lock in MergeTree tables", ValueType::Microseconds) \
    M(PartsLockWaitMicroseconds, "Total time spent waiting for data parts lock in MergeTree tables", ValueType::Microseconds) \
    \
    M(RealTimeMicroseconds, "Total (wall clock) time spent in processing (queries and other tasks) threads (note that this is a sum).", ValueType::Microseconds) \
    M(UserTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in user mode. This includes time CPU pipeline was stalled due to main memory access, cache misses, branch mispredictions, hyper-threading, etc.", ValueType::Microseconds) \
    M(SystemTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in OS kernel mode. This is time spent in syscalls, excluding waiting time during blocking syscalls.", ValueType::Microseconds) \
    M(MemoryOvercommitWaitTimeMicroseconds, "Total time spent in waiting for memory to be freed in OvercommitTracker.", ValueType::Microseconds) \
    M(MemoryAllocatorPurge, "Total number of times memory allocator purge was requested", ValueType::Number) \
    M(MemoryAllocatorPurgeTimeMicroseconds, "Total time spent for memory allocator purge", ValueType::Microseconds) \
    M(SoftPageFaults, "The number of soft page faults in query execution threads. Soft page fault usually means a miss in the memory allocator cache, which requires a new memory mapping from the OS and subsequent allocation of a page of physical memory.", ValueType::Number) \
    M(HardPageFaults, "The number of hard page faults in query execution threads. High values indicate either that you forgot to turn off swap on your server, or eviction of memory pages of the ClickHouse binary during very high memory pressure, or successful usage of the 'mmap' read method for the tables data.", ValueType::Number) \
    \
    M(OSIOWaitMicroseconds, "Total time a thread spent waiting for a result of IO operation, from the OS point of view. This is real IO that doesn't include page cache.", ValueType::Microseconds) \
    M(OSCPUWaitMicroseconds, "Total time a thread was ready for execution but waiting to be scheduled by OS, from the OS point of view.", ValueType::Microseconds) \
    M(OSCPUVirtualTimeMicroseconds, "CPU time spent seen by OS. Does not include involuntary waits due to virtualization.", ValueType::Microseconds) \
    M(OSReadBytes, "Number of bytes read from disks or block devices. Doesn't include bytes read from page cache. May include excessive data due to block size, readahead, etc.", ValueType::Bytes) \
    M(OSWriteBytes, "Number of bytes written to disks or block devices. Doesn't include bytes that are in page cache dirty pages. May not include data that was written by OS asynchronously.", ValueType::Bytes) \
    M(OSReadChars, "Number of bytes read from filesystem, including page cache.", ValueType::Bytes) \
    M(OSWriteChars, "Number of bytes written to filesystem, including page cache.", ValueType::Bytes) \
    \
    M(ParallelReplicasHandleRequestMicroseconds, "Time spent processing requests for marks from replicas", ValueType::Microseconds) \
    M(ParallelReplicasHandleAnnouncementMicroseconds, "Time spent processing replicas announcements", ValueType::Microseconds) \
    M(ParallelReplicasAnnouncementMicroseconds, "Time spent to send an announcement", ValueType::Microseconds) \
    M(ParallelReplicasReadRequestMicroseconds, "Time spent for read requests", ValueType::Microseconds) \
    \
    M(ParallelReplicasReadAssignedMarks, "Sum across all replicas of how many of scheduled marks were assigned by consistent hash", ValueType::Number) \
    M(ParallelReplicasReadUnassignedMarks, "Sum across all replicas of how many unassigned marks were scheduled", ValueType::Number) \
    M(ParallelReplicasReadAssignedForStealingMarks, "Sum across all replicas of how many of scheduled marks were assigned for stealing by consistent hash", ValueType::Number) \
    M(ParallelReplicasReadMarks, "How many marks were read by the given replica", ValueType::Number) \
    \
    M(ParallelReplicasStealingByHashMicroseconds, "Time spent collecting segments meant for stealing by hash", ValueType::Microseconds) \
    M(ParallelReplicasProcessingPartsMicroseconds, "Time spent processing data parts", ValueType::Microseconds) \
    M(ParallelReplicasStealingLeftoversMicroseconds, "Time spent collecting orphaned segments", ValueType::Microseconds) \
    M(ParallelReplicasCollectingOwnedSegmentsMicroseconds, "Time spent collecting segments meant by hash", ValueType::Microseconds) \
    M(ParallelReplicasNumRequests, "Number of requests to the initiator.", ValueType::Number) \
    M(ParallelReplicasDeniedRequests, "Number of completely denied requests to the initiator", ValueType::Number) \
    M(CacheWarmerBytesDownloaded, "Amount of data fetched into filesystem cache by dedicated background threads.", ValueType::Bytes) \
    M(CacheWarmerDataPartsDownloaded, "Number of data parts that were fully fetched by CacheWarmer.", ValueType::Number) \
    M(IgnoredColdParts, "See setting ignore_cold_parts_seconds. Number of times read queries ignored very new parts that weren't pulled into cache by CacheWarmer yet.", ValueType::Number) \
    M(PreferredWarmedUnmergedParts, "See setting prefer_warmed_unmerged_parts_seconds. Number of times read queries used outdated pre-merge parts that are in cache instead of merged part that wasn't pulled into cache by CacheWarmer yet.", ValueType::Number) \
    \
    M(PerfCPUCycles, "Total cycles. Be wary of what happens during CPU frequency scaling.", ValueType::Number) \
    M(PerfInstructions, "Retired instructions. Be careful, these can be affected by various issues, most notably hardware interrupt counts.", ValueType::Number) \
    M(PerfCacheReferences, "Cache accesses. Usually, this indicates Last Level Cache accesses, but this may vary depending on your CPU. This may include prefetches and coherency messages; again this depends on the design of your CPU.", ValueType::Number) \
    M(PerfCacheMisses, "Cache misses. Usually this indicates Last Level Cache misses; this is intended to be used in conjunction with the PERFCOUNTHWCACHEREFERENCES event to calculate cache miss rates.", ValueType::Number) \
    M(PerfBranchInstructions, "Retired branch instructions. Prior to Linux 2.6.35, this used the wrong event on AMD processors.", ValueType::Number) \
    M(PerfBranchMisses, "Mispredicted branch instructions.", ValueType::Number) \
    M(PerfBusCycles, "Bus cycles, which can be different from total cycles.", ValueType::Number) \
    M(PerfStalledCyclesFrontend, "Stalled cycles during issue.", ValueType::Number) \
    M(PerfStalledCyclesBackend, "Stalled cycles during retirement.", ValueType::Number) \
    M(PerfRefCPUCycles, "Total cycles; not affected by CPU frequency scaling.", ValueType::Number) \
    \
    M(PerfCPUClock, "The CPU clock, a high-resolution per-CPU timer", ValueType::Number) \
    M(PerfTaskClock, "A clock count specific to the task that is running", ValueType::Number) \
    M(PerfContextSwitches, "Number of context switches", ValueType::Number) \
    M(PerfCPUMigrations, "Number of times the process has migrated to a new CPU", ValueType::Number) \
    M(PerfAlignmentFaults, "Number of alignment faults. These happen when unaligned memory accesses happen; the kernel can handle these but it reduces performance. This happens only on some architectures (never on x86).", ValueType::Number) \
    M(PerfEmulationFaults, "Number of emulation faults. The kernel sometimes traps on unimplemented instructions and emulates them for user space. This can negatively impact performance.", ValueType::Number) \
    M(PerfMinEnabledTime, "For all events, minimum time that an event was enabled. Used to track event multiplexing influence", ValueType::Number) \
    M(PerfMinEnabledRunningTime, "Running time for event with minimum enabled time. Used to track the amount of event multiplexing", ValueType::Number) \
    M(PerfDataTLBReferences, "Data TLB references", ValueType::Number) \
    M(PerfDataTLBMisses, "Data TLB misses", ValueType::Number) \
    M(PerfInstructionTLBReferences, "Instruction TLB references", ValueType::Number) \
    M(PerfInstructionTLBMisses, "Instruction TLB misses", ValueType::Number) \
    M(PerfLocalMemoryReferences, "Local NUMA node memory reads", ValueType::Number) \
    M(PerfLocalMemoryMisses, "Local NUMA node memory read misses", ValueType::Number) \
    \
    M(CannotWriteToWriteBufferDiscard, "Number of stack traces dropped by query profiler or signal handler because pipe is full or cannot write to pipe.", ValueType::Number) \
    M(QueryProfilerSignalOverruns, "Number of times we drop processing of a query profiler signal due to overrun plus the number of signals that OS has not delivered due to overrun.", ValueType::Number) \
    M(QueryProfilerConcurrencyOverruns, "Number of times we drop processing of a query profiler signal due to too many concurrent query profilers in other threads, which may indicate overload.", ValueType::Number) \
    M(QueryProfilerRuns, "Number of times QueryProfiler had been run.", ValueType::Number) \
    M(QueryProfilerErrors, "Invalid memory accesses during asynchronous stack unwinding.", ValueType::Number) \
    \
    M(CreatedLogEntryForMerge, "Successfully created log entry to merge parts in ReplicatedMergeTree.", ValueType::Number) \
    M(NotCreatedLogEntryForMerge, "Log entry to merge parts in ReplicatedMergeTree is not created due to concurrent log update by another replica.", ValueType::Number) \
    M(CreatedLogEntryForMutation, "Successfully created log entry to mutate parts in ReplicatedMergeTree.", ValueType::Number) \
    M(NotCreatedLogEntryForMutation, "Log entry to mutate parts in ReplicatedMergeTree is not created due to concurrent log update by another replica.", ValueType::Number) \
    \
    M(S3ReadMicroseconds, "Time of GET and HEAD requests to S3 storage.", ValueType::Microseconds) \
    M(S3ReadRequestsCount, "Number of GET and HEAD requests to S3 storage.", ValueType::Number) \
    M(S3ReadRequestsErrors, "Number of non-throttling errors in GET and HEAD requests to S3 storage.", ValueType::Number) \
    M(S3ReadRequestsThrottling, "Number of 429 and 503 errors in GET and HEAD requests to S3 storage.", ValueType::Number) \
    M(S3ReadRequestsRedirects, "Number of redirects in GET and HEAD requests to S3 storage.", ValueType::Number) \
    \
    M(S3WriteMicroseconds, "Time of POST, DELETE, PUT and PATCH requests to S3 storage.", ValueType::Microseconds) \
    M(S3WriteRequestsCount, "Number of POST, DELETE, PUT and PATCH requests to S3 storage.", ValueType::Number) \
    M(S3WriteRequestsErrors, "Number of non-throttling errors in POST, DELETE, PUT and PATCH requests to S3 storage.", ValueType::Number) \
    M(S3WriteRequestsThrottling, "Number of 429 and 503 errors in POST, DELETE, PUT and PATCH requests to S3 storage.", ValueType::Number) \
    M(S3WriteRequestsRedirects, "Number of redirects in POST, DELETE, PUT and PATCH requests to S3 storage.", ValueType::Number) \
    \
    M(DiskS3ReadMicroseconds, "Time of GET and HEAD requests to DiskS3 storage.", ValueType::Microseconds) \
    M(DiskS3ReadRequestsCount, "Number of GET and HEAD requests to DiskS3 storage.", ValueType::Number) \
    M(DiskS3ReadRequestsErrors, "Number of non-throttling errors in GET and HEAD requests to DiskS3 storage.", ValueType::Number) \
    M(DiskS3ReadRequestsThrottling, "Number of 429 and 503 errors in GET and HEAD requests to DiskS3 storage.", ValueType::Number) \
    M(DiskS3ReadRequestsRedirects, "Number of redirects in GET and HEAD requests to DiskS3 storage.", ValueType::Number) \
    \
    M(DiskS3WriteMicroseconds, "Time of POST, DELETE, PUT and PATCH requests to DiskS3 storage.", ValueType::Microseconds) \
    M(DiskS3WriteRequestsCount, "Number of POST, DELETE, PUT and PATCH requests to DiskS3 storage.", ValueType::Number) \
    M(DiskS3WriteRequestsErrors, "Number of non-throttling errors in POST, DELETE, PUT and PATCH requests to DiskS3 storage.", ValueType::Number) \
    M(DiskS3WriteRequestsThrottling, "Number of 429 and 503 errors in POST, DELETE, PUT and PATCH requests to DiskS3 storage.", ValueType::Number) \
    M(DiskS3WriteRequestsRedirects, "Number of redirects in POST, DELETE, PUT and PATCH requests to DiskS3 storage.", ValueType::Number) \
    \
    M(S3DeleteObjects, "Number of S3 API DeleteObject(s) calls.", ValueType::Number) \
    M(S3CopyObject, "Number of S3 API CopyObject calls.", ValueType::Number) \
    M(S3ListObjects, "Number of S3 API ListObjects calls.", ValueType::Number) \
    M(S3HeadObject,  "Number of S3 API HeadObject calls.", ValueType::Number) \
    M(S3GetObjectAttributes, "Number of S3 API GetObjectAttributes calls.", ValueType::Number) \
    M(S3CreateMultipartUpload, "Number of S3 API CreateMultipartUpload calls.", ValueType::Number) \
    M(S3UploadPartCopy, "Number of S3 API UploadPartCopy calls.", ValueType::Number) \
    M(S3UploadPart, "Number of S3 API UploadPart calls.", ValueType::Number) \
    M(S3AbortMultipartUpload, "Number of S3 API AbortMultipartUpload calls.", ValueType::Number) \
    M(S3CompleteMultipartUpload, "Number of S3 API CompleteMultipartUpload calls.", ValueType::Number) \
    M(S3PutObject, "Number of S3 API PutObject calls.", ValueType::Number) \
    M(S3GetObject, "Number of S3 API GetObject calls.", ValueType::Number) \
    \
    M(DiskS3DeleteObjects, "Number of DiskS3 API DeleteObject(s) calls.", ValueType::Number) \
    M(DiskS3CopyObject, "Number of DiskS3 API CopyObject calls.", ValueType::Number) \
    M(DiskS3ListObjects, "Number of DiskS3 API ListObjects calls.", ValueType::Number) \
    M(DiskS3HeadObject,  "Number of DiskS3 API HeadObject calls.", ValueType::Number) \
    M(DiskS3GetObjectAttributes, "Number of DiskS3 API GetObjectAttributes calls.", ValueType::Number) \
    M(DiskS3CreateMultipartUpload, "Number of DiskS3 API CreateMultipartUpload calls.", ValueType::Number) \
    M(DiskS3UploadPartCopy, "Number of DiskS3 API UploadPartCopy calls.", ValueType::Number) \
    M(DiskS3UploadPart, "Number of DiskS3 API UploadPart calls.", ValueType::Number) \
    M(DiskS3AbortMultipartUpload, "Number of DiskS3 API AbortMultipartUpload calls.", ValueType::Number) \
    M(DiskS3CompleteMultipartUpload, "Number of DiskS3 API CompleteMultipartUpload calls.", ValueType::Number) \
    M(DiskS3PutObject, "Number of DiskS3 API PutObject calls.", ValueType::Number) \
    M(DiskS3GetObject, "Number of DiskS3 API GetObject calls.", ValueType::Number) \
    \
    M(DiskPlainRewritableAzureDirectoryCreated, "Number of directories created by the 'plain_rewritable' metadata storage for AzureObjectStorage.", ValueType::Number) \
    M(DiskPlainRewritableAzureDirectoryRemoved, "Number of directories removed by the 'plain_rewritable' metadata storage for AzureObjectStorage.", ValueType::Number) \
    M(DiskPlainRewritableLocalDirectoryCreated, "Number of directories created by the 'plain_rewritable' metadata storage for LocalObjectStorage.", ValueType::Number) \
    M(DiskPlainRewritableLocalDirectoryRemoved, "Number of directories removed by the 'plain_rewritable' metadata storage for LocalObjectStorage.", ValueType::Number) \
    M(DiskPlainRewritableS3DirectoryCreated, "Number of directories created by the 'plain_rewritable' metadata storage for S3ObjectStorage.", ValueType::Number) \
    M(DiskPlainRewritableS3DirectoryRemoved, "Number of directories removed by the 'plain_rewritable' metadata storage for S3ObjectStorage.", ValueType::Number) \
    M(DiskPlainRewritableLegacyLayoutDiskCount, "Number of the 'plain_rewritable' disks with legacy layout.", ValueType::Number) \
    \
    M(S3Clients, "Number of created S3 clients.", ValueType::Number) \
    M(TinyS3Clients, "Number of S3 clients copies which reuse an existing auth provider from another client.", ValueType::Number) \
    \
    M(EngineFileLikeReadFiles, "Number of files read in table engines working with files (like File/S3/URL/HDFS).", ValueType::Number) \
    \
    M(ReadBufferFromS3Microseconds, "Time spent on reading from S3.", ValueType::Microseconds) \
    M(ReadBufferFromS3InitMicroseconds, "Time spent initializing connection to S3.", ValueType::Microseconds) \
    M(ReadBufferFromS3Bytes, "Bytes read from S3.", ValueType::Bytes) \
    M(ReadBufferFromS3RequestsErrors, "Number of exceptions while reading from S3.", ValueType::Number) \
    \
    M(WriteBufferFromS3Microseconds, "Time spent on writing to S3.", ValueType::Microseconds) \
    M(WriteBufferFromS3Bytes, "Bytes written to S3.", ValueType::Bytes) \
    M(WriteBufferFromS3RequestsErrors, "Number of exceptions while writing to S3.", ValueType::Number) \
    M(WriteBufferFromS3WaitInflightLimitMicroseconds, "Time spent on waiting while some of the current requests are done when its number reached the limit defined by s3_max_inflight_parts_for_one_file.", ValueType::Microseconds) \
    M(QueryMemoryLimitExceeded, "Number of times when memory limit exceeded for query.", ValueType::Number) \
    \
    M(AzureGetObject, "Number of Azure API GetObject calls.", ValueType::Number) \
    M(AzureUpload, "Number of Azure blob storage API Upload calls", ValueType::Number) \
    M(AzureStageBlock, "Number of Azure blob storage API StageBlock calls", ValueType::Number) \
    M(AzureCommitBlockList, "Number of Azure blob storage API CommitBlockList calls", ValueType::Number) \
    M(AzureCopyObject, "Number of Azure blob storage API CopyObject calls", ValueType::Number) \
    M(AzureDeleteObjects, "Number of Azure blob storage API DeleteObject(s) calls.", ValueType::Number) \
    M(AzureListObjects, "Number of Azure blob storage API ListObjects calls.", ValueType::Number) \
    M(AzureGetProperties, "Number of Azure blob storage API GetProperties calls.", ValueType::Number) \
    M(AzureCreateContainer, "Number of Azure blob storage API CreateContainer calls.", ValueType::Number) \
    \
    M(DiskAzureGetObject, "Number of Disk Azure API GetObject calls.", ValueType::Number) \
    M(DiskAzureUpload, "Number of Disk Azure blob storage API Upload calls", ValueType::Number) \
    M(DiskAzureStageBlock, "Number of Disk Azure blob storage API StageBlock calls", ValueType::Number) \
    M(DiskAzureCommitBlockList, "Number of Disk Azure blob storage API CommitBlockList calls", ValueType::Number) \
    M(DiskAzureCopyObject, "Number of Disk Azure blob storage API CopyObject calls", ValueType::Number) \
    M(DiskAzureListObjects, "Number of Disk Azure blob storage API ListObjects calls.", ValueType::Number) \
    M(DiskAzureDeleteObjects, "Number of Azure blob storage API DeleteObject(s) calls.", ValueType::Number) \
    M(DiskAzureGetProperties, "Number of Disk Azure blob storage API GetProperties calls.", ValueType::Number) \
    M(DiskAzureCreateContainer, "Number of Disk Azure blob storage API CreateContainer calls.", ValueType::Number) \
    \
    M(ReadBufferFromAzureMicroseconds, "Time spent on reading from Azure.", ValueType::Microseconds) \
    M(ReadBufferFromAzureInitMicroseconds, "Time spent initializing connection to Azure.", ValueType::Microseconds) \
    M(ReadBufferFromAzureBytes, "Bytes read from Azure.", ValueType::Bytes) \
    M(ReadBufferFromAzureRequestsErrors, "Number of exceptions while reading from Azure", ValueType::Number) \
    \
    M(CachedReadBufferReadFromCacheHits, "Number of times the read from filesystem cache hit the cache.", ValueType::Number) \
    M(CachedReadBufferReadFromCacheMisses, "Number of times the read from filesystem cache miss the cache.", ValueType::Number) \
    M(CachedReadBufferReadFromSourceMicroseconds, "Time reading from filesystem cache source (from remote filesystem, etc)", ValueType::Microseconds) \
    M(CachedReadBufferReadFromCacheMicroseconds, "Time reading from filesystem cache", ValueType::Microseconds) \
    M(CachedReadBufferReadFromSourceBytes, "Bytes read from filesystem cache source (from remote fs, etc)", ValueType::Bytes) \
    M(CachedReadBufferReadFromCacheBytes, "Bytes read from filesystem cache", ValueType::Bytes) \
    M(CachedReadBufferPredownloadedBytes, "Bytes read from filesystem cache source. Cache segments are read from left to right as a whole, it might be that we need to predownload some part of the segment irrelevant for the current task just to get to the needed data", ValueType::Bytes) \
    M(CachedReadBufferCacheWriteBytes, "Bytes written from source (remote fs, etc) to filesystem cache", ValueType::Bytes) \
    M(CachedReadBufferCacheWriteMicroseconds, "Time spent writing data into filesystem cache", ValueType::Microseconds) \
    M(CachedReadBufferCreateBufferMicroseconds, "Prepare buffer time", ValueType::Microseconds) \
    M(CachedWriteBufferCacheWriteBytes, "Bytes written from source (remote fs, etc) to filesystem cache", ValueType::Bytes) \
    M(CachedWriteBufferCacheWriteMicroseconds, "Time spent writing data into filesystem cache", ValueType::Microseconds) \
    \
    M(FilesystemCacheLoadMetadataMicroseconds, "Time spent loading filesystem cache metadata", ValueType::Microseconds) \
    M(FilesystemCacheEvictedBytes, "Number of bytes evicted from filesystem cache", ValueType::Bytes) \
    M(FilesystemCacheCreatedKeyDirectories, "Number of created key directories", ValueType::Bytes) \
    M(FilesystemCacheEvictedFileSegments, "Number of file segments evicted from filesystem cache", ValueType::Number) \
    M(FilesystemCacheEvictedFileSegmentsDuringPriorityIncrease, "Number of file segments evicted from filesystem cache when increasing priority of file segments (Applies to SLRU cache policy)", ValueType::Number) \
    M(FilesystemCacheBackgroundDownloadQueuePush, "Number of file segments sent for background download in filesystem cache", ValueType::Number) \
    M(FilesystemCacheEvictionSkippedFileSegments, "Number of file segments skipped for eviction because of being in unreleasable state", ValueType::Number) \
    M(FilesystemCacheEvictionSkippedEvictingFileSegments, "Number of file segments skipped for eviction because of being in evicting state", ValueType::Number) \
    M(FilesystemCacheEvictionTries, "Number of filesystem cache eviction attempts", ValueType::Number) \
    M(FilesystemCacheLockKeyMicroseconds, "Lock cache key time", ValueType::Microseconds) \
    M(FilesystemCacheLockMetadataMicroseconds, "Lock filesystem cache metadata time", ValueType::Microseconds) \
    M(FilesystemCacheLockCacheMicroseconds, "Lock filesystem cache time", ValueType::Microseconds) \
    M(FilesystemCacheReserveMicroseconds, "Filesystem cache space reservation time", ValueType::Microseconds) \
    M(FilesystemCacheEvictMicroseconds, "Filesystem cache eviction time", ValueType::Microseconds) \
    M(FilesystemCacheGetOrSetMicroseconds, "Filesystem cache getOrSet() time", ValueType::Microseconds) \
    M(FilesystemCacheGetMicroseconds, "Filesystem cache get() time", ValueType::Microseconds) \
    M(FileSegmentWaitMicroseconds, "Wait on DOWNLOADING state", ValueType::Microseconds) \
    M(FileSegmentCompleteMicroseconds, "Duration of FileSegment::complete() in filesystem cache", ValueType::Microseconds) \
    M(FileSegmentLockMicroseconds, "Lock file segment time", ValueType::Microseconds) \
    M(FileSegmentWriteMicroseconds, "File segment write() time", ValueType::Microseconds) \
    M(FileSegmentUseMicroseconds, "File segment use() time", ValueType::Microseconds) \
    M(FileSegmentRemoveMicroseconds, "File segment remove() time", ValueType::Microseconds) \
    M(FileSegmentHolderCompleteMicroseconds, "File segments holder complete() time", ValueType::Microseconds) \
    M(FileSegmentFailToIncreasePriority, "Number of times the priority was not increased due to a high contention on the cache lock", ValueType::Number) \
    M(FilesystemCacheFailToReserveSpaceBecauseOfLockContention, "Number of times space reservation was skipped due to a high contention on the cache lock", ValueType::Number) \
    M(FilesystemCacheFailToReserveSpaceBecauseOfCacheResize, "Number of times space reservation was skipped due to the cache is being resized", ValueType::Number) \
    M(FilesystemCacheHoldFileSegments, "Filesystem cache file segments count, which were hold", ValueType::Number) \
    M(FilesystemCacheUnusedHoldFileSegments, "Filesystem cache file segments count, which were hold, but not used (because of seek or LIMIT n, etc)", ValueType::Number) \
    M(FilesystemCacheFreeSpaceKeepingThreadRun, "Number of times background thread executed free space keeping job", ValueType::Number) \
    M(FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds, "Time for which background thread executed free space keeping job", ValueType::Milliseconds) \
    M(FilesystemCacheFailedEvictionCandidates, "Number of file segments which unexpectedly failed to be evicted during dynamic filesystem cache eviction", ValueType::Number) \
    \
    M(RemoteFSSeeks, "Total number of seeks for async buffer", ValueType::Number) \
    M(RemoteFSPrefetches, "Number of prefetches made with asynchronous reading from remote filesystem", ValueType::Number) \
    M(RemoteFSCancelledPrefetches, "Number of cancelled prefecthes (because of seek)", ValueType::Number) \
    M(RemoteFSUnusedPrefetches, "Number of prefetches pending at buffer destruction", ValueType::Number) \
    M(RemoteFSPrefetchedReads, "Number of reads from prefecthed buffer", ValueType::Number) \
    M(RemoteFSPrefetchedBytes, "Number of bytes from prefecthed buffer", ValueType::Bytes) \
    M(RemoteFSUnprefetchedReads, "Number of reads from unprefetched buffer", ValueType::Number) \
    M(RemoteFSUnprefetchedBytes, "Number of bytes from unprefetched buffer", ValueType::Bytes) \
    M(RemoteFSLazySeeks, "Number of lazy seeks", ValueType::Number) \
    M(RemoteFSSeeksWithReset, "Number of seeks which lead to a new connection", ValueType::Number) \
    M(RemoteFSBuffers, "Number of buffers created for asynchronous reading from remote filesystem", ValueType::Number) \
    M(MergeTreePrefetchedReadPoolInit, "Time spent preparing tasks in MergeTreePrefetchedReadPool", ValueType::Microseconds) \
    M(WaitPrefetchTaskMicroseconds, "Time spend waiting for prefetched reader", ValueType::Microseconds) \
    \
    M(ThreadpoolReaderTaskMicroseconds, "Time spent getting the data in asynchronous reading", ValueType::Microseconds) \
    M(ThreadpoolReaderPrepareMicroseconds, "Time spent on preparation (e.g. call to reader seek() method)", ValueType::Microseconds) \
    M(ThreadpoolReaderReadBytes, "Bytes read from a threadpool task in asynchronous reading", ValueType::Bytes) \
    M(ThreadpoolReaderSubmit, "Bytes read from a threadpool task in asynchronous reading", ValueType::Bytes) \
    M(ThreadpoolReaderSubmitReadSynchronously, "How many times we haven't scheduled a task on the thread pool and read synchronously instead", ValueType::Number) \
    M(ThreadpoolReaderSubmitReadSynchronouslyBytes, "How many bytes were read synchronously", ValueType::Bytes) \
    M(ThreadpoolReaderSubmitReadSynchronouslyMicroseconds, "How much time we spent reading synchronously", ValueType::Microseconds) \
    M(ThreadpoolReaderSubmitLookupInCacheMicroseconds, "How much time we spent checking if content is cached", ValueType::Microseconds) \
    M(AsynchronousReaderIgnoredBytes, "Number of bytes ignored during asynchronous reading", ValueType::Bytes) \
    \
    M(FileSegmentWaitReadBufferMicroseconds, "Metric per file segment. Time spend waiting for internal read buffer (includes cache waiting)", ValueType::Microseconds) \
    M(FileSegmentReadMicroseconds, "Metric per file segment. Time spend reading from file", ValueType::Microseconds) \
    M(FileSegmentCacheWriteMicroseconds, "Metric per file segment. Time spend writing data to cache", ValueType::Microseconds) \
    M(FileSegmentPredownloadMicroseconds, "Metric per file segment. Time spent pre-downloading data to cache (pre-downloading - finishing file segment download (after someone who failed to do that) up to the point current thread was requested to do)", ValueType::Microseconds) \
    M(FileSegmentUsedBytes, "Metric per file segment. How many bytes were actually used from current file segment", ValueType::Bytes) \
    \
    M(ReadBufferSeekCancelConnection, "Number of seeks which lead to new connection (s3, http)", ValueType::Number) \
    \
    M(SleepFunctionCalls, "Number of times a sleep function (sleep, sleepEachRow) has been called.", ValueType::Number) \
    M(SleepFunctionMicroseconds, "Time set to sleep in a sleep function (sleep, sleepEachRow).", ValueType::Microseconds) \
    M(SleepFunctionElapsedMicroseconds, "Time spent sleeping in a sleep function (sleep, sleepEachRow).", ValueType::Microseconds) \
    \
    M(ThreadPoolReaderPageCacheHit, "Number of times the read inside ThreadPoolReader was done from the page cache.", ValueType::Number) \
    M(ThreadPoolReaderPageCacheHitBytes, "Number of bytes read inside ThreadPoolReader when it was done from the page cache.", ValueType::Bytes) \
    M(ThreadPoolReaderPageCacheHitElapsedMicroseconds, "Time spent reading data from page cache in ThreadPoolReader.", ValueType::Microseconds) \
    M(ThreadPoolReaderPageCacheMiss, "Number of times the read inside ThreadPoolReader was not done from page cache and was hand off to thread pool.", ValueType::Number) \
    M(ThreadPoolReaderPageCacheMissBytes, "Number of bytes read inside ThreadPoolReader when read was not done from page cache and was hand off to thread pool.", ValueType::Bytes) \
    M(ThreadPoolReaderPageCacheMissElapsedMicroseconds, "Time spent reading data inside the asynchronous job in ThreadPoolReader - when read was not done from the page cache.", ValueType::Microseconds) \
    \
    M(AsynchronousReadWaitMicroseconds, "Time spent in waiting for asynchronous reads in asynchronous local read.", ValueType::Microseconds) \
    M(SynchronousReadWaitMicroseconds, "Time spent in waiting for synchronous reads in asynchronous local read.", ValueType::Microseconds) \
    M(AsynchronousRemoteReadWaitMicroseconds, "Time spent in waiting for asynchronous remote reads.", ValueType::Microseconds) \
    M(SynchronousRemoteReadWaitMicroseconds, "Time spent in waiting for synchronous remote reads.", ValueType::Microseconds) \
    \
    M(ExternalDataSourceLocalCacheReadBytes, "Bytes read from local cache buffer in RemoteReadBufferCache", ValueType::Bytes) \
    \
    M(MainConfigLoads, "Number of times the main configuration was reloaded.", ValueType::Number) \
    \
    M(AggregationPreallocatedElementsInHashTables, "How many elements were preallocated in hash tables for aggregation.", ValueType::Number) \
    M(AggregationHashTablesInitializedAsTwoLevel, "How many hash tables were inited as two-level for aggregation.", ValueType::Number) \
    M(AggregationOptimizedEqualRangesOfKeys, "For how many blocks optimization of equal ranges of keys was applied", ValueType::Number) \
    M(HashJoinPreallocatedElementsInHashTables, "How many elements were preallocated in hash tables for hash join.", ValueType::Number) \
    \
    M(MetadataFromKeeperCacheHit, "Number of times an object storage metadata request was answered from cache without making request to Keeper", ValueType::Number) \
    M(MetadataFromKeeperCacheMiss, "Number of times an object storage metadata request had to be answered from Keeper", ValueType::Number) \
    M(MetadataFromKeeperCacheUpdateMicroseconds, "Total time spent in updating the cache including waiting for responses from Keeper", ValueType::Microseconds) \
    M(MetadataFromKeeperUpdateCacheOneLevel, "Number of times a cache update for one level of directory tree was done", ValueType::Number) \
    M(MetadataFromKeeperTransactionCommit, "Number of times metadata transaction commit was attempted", ValueType::Number) \
    M(MetadataFromKeeperTransactionCommitRetry, "Number of times metadata transaction commit was retried", ValueType::Number) \
    M(MetadataFromKeeperCleanupTransactionCommit, "Number of times metadata transaction commit for deleted objects cleanup was attempted", ValueType::Number) \
    M(MetadataFromKeeperCleanupTransactionCommitRetry, "Number of times metadata transaction commit for deleted objects cleanup was retried", ValueType::Number) \
    M(MetadataFromKeeperOperations, "Number of times a request was made to Keeper", ValueType::Number) \
    M(MetadataFromKeeperIndividualOperations, "Number of paths read or written by single or multi requests to Keeper", ValueType::Number) \
    M(MetadataFromKeeperReconnects, "Number of times a reconnect to Keeper was done", ValueType::Number) \
    M(MetadataFromKeeperBackgroundCleanupObjects, "Number of times a old deleted object clean up was performed by background task", ValueType::Number) \
    M(MetadataFromKeeperBackgroundCleanupTransactions, "Number of times old transaction idempotency token was cleaned up by background task", ValueType::Number) \
    M(MetadataFromKeeperBackgroundCleanupErrors, "Number of times an error was encountered in background cleanup task", ValueType::Number) \
    \
    M(SharedMergeTreeMetadataCacheHintLoadedFromCache, "Number of times metadata cache hint was found without going to Keeper", ValueType::Number) \
    \
    M(KafkaRebalanceRevocations, "Number of partition revocations (the first stage of consumer group rebalance)", ValueType::Number) \
    M(KafkaRebalanceAssignments, "Number of partition assignments (the final stage of consumer group rebalance)", ValueType::Number) \
    M(KafkaRebalanceErrors, "Number of failed consumer group rebalances", ValueType::Number) \
    M(KafkaMessagesPolled, "Number of Kafka messages polled from librdkafka to ClickHouse", ValueType::Number) \
    M(KafkaMessagesRead, "Number of Kafka messages already processed by ClickHouse", ValueType::Number) \
    M(KafkaMessagesFailed, "Number of Kafka messages ClickHouse failed to parse", ValueType::Number) \
    M(KafkaRowsRead, "Number of rows parsed from Kafka messages", ValueType::Number) \
    M(KafkaRowsRejected, "Number of parsed rows which were later rejected (due to rebalances / errors or similar reasons). Those rows will be consumed again after the rebalance.", ValueType::Number) \
    M(KafkaDirectReads, "Number of direct selects from Kafka tables since server start", ValueType::Number) \
    M(KafkaBackgroundReads, "Number of background reads populating materialized views from Kafka since server start", ValueType::Number) \
    M(KafkaCommits, "Number of successful commits of consumed offsets to Kafka (normally should be the same as KafkaBackgroundReads)", ValueType::Number) \
    M(KafkaCommitFailures, "Number of failed commits of consumed offsets to Kafka (usually is a sign of some data duplication)", ValueType::Number) \
    M(KafkaConsumerErrors, "Number of errors reported by librdkafka during polls", ValueType::Number) \
    M(KafkaWrites, "Number of writes (inserts) to Kafka tables ", ValueType::Number) \
    M(KafkaRowsWritten, "Number of rows inserted into Kafka tables", ValueType::Number) \
    M(KafkaProducerFlushes, "Number of explicit flushes to Kafka producer", ValueType::Number) \
    M(KafkaMessagesProduced, "Number of messages produced to Kafka", ValueType::Number) \
    M(KafkaProducerErrors, "Number of errors during producing the messages to Kafka", ValueType::Number) \
    \
    M(ScalarSubqueriesGlobalCacheHit, "Number of times a read from a scalar subquery was done using the global cache", ValueType::Number) \
    M(ScalarSubqueriesLocalCacheHit, "Number of times a read from a scalar subquery was done using the local cache", ValueType::Number) \
    M(ScalarSubqueriesCacheMiss, "Number of times a read from a scalar subquery was not cached and had to be calculated completely", ValueType::Number) \
    \
    M(SchemaInferenceCacheHits, "Number of times the requested source is found in schema cache", ValueType::Number) \
    M(SchemaInferenceCacheSchemaHits, "Number of times the schema is found in schema cache during schema inference", ValueType::Number) \
    M(SchemaInferenceCacheNumRowsHits, "Number of times the number of rows is found in schema cache during count from files", ValueType::Number) \
    M(SchemaInferenceCacheMisses, "Number of times the requested source is not in schema cache", ValueType::Number) \
    M(SchemaInferenceCacheSchemaMisses, "Number of times the requested source is in cache but the schema is not in cache during schema inference", ValueType::Number) \
    M(SchemaInferenceCacheNumRowsMisses, "Number of times the requested source is in cache but the number of rows is not in cache while count from files", ValueType::Number) \
    M(SchemaInferenceCacheEvictions, "Number of times a schema from cache was evicted due to overflow", ValueType::Number) \
    M(SchemaInferenceCacheInvalidations, "Number of times a schema in cache became invalid due to changes in data", ValueType::Number) \
    \
    M(KeeperPacketsSent, "Packets sent by keeper server", ValueType::Number) \
    M(KeeperPacketsReceived, "Packets received by keeper server", ValueType::Number) \
    M(KeeperRequestTotal, "Total requests number on keeper server", ValueType::Number) \
    M(KeeperLatency, "Keeper latency", ValueType::Milliseconds) \
    M(KeeperTotalElapsedMicroseconds, "Keeper total latency for a single request", ValueType::Microseconds) \
    M(KeeperProcessElapsedMicroseconds, "Keeper commit latency for a single request", ValueType::Microseconds) \
    M(KeeperPreprocessElapsedMicroseconds, "Keeper preprocessing latency for a single reuquest", ValueType::Microseconds) \
    M(KeeperStorageLockWaitMicroseconds, "Time spent waiting for acquiring Keeper storage lock", ValueType::Microseconds) \
    M(KeeperCommitWaitElapsedMicroseconds, "Time spent waiting for certain log to be committed", ValueType::Microseconds) \
    M(KeeperBatchMaxCount, "Number of times the size of batch was limited by the amount", ValueType::Number) \
    M(KeeperBatchMaxTotalSize, "Number of times the size of batch was limited by the total bytes size", ValueType::Number) \
    M(KeeperCommits, "Number of successful commits", ValueType::Number) \
    M(KeeperCommitsFailed, "Number of failed commits", ValueType::Number) \
    M(KeeperSnapshotCreations, "Number of snapshots creations", ValueType::Number) \
    M(KeeperSnapshotCreationsFailed, "Number of failed snapshot creations", ValueType::Number) \
    M(KeeperSnapshotApplys, "Number of snapshot applying", ValueType::Number) \
    M(KeeperSnapshotApplysFailed, "Number of failed snapshot applying", ValueType::Number) \
    M(KeeperReadSnapshot, "Number of snapshot read(serialization)", ValueType::Number) \
    M(KeeperSaveSnapshot, "Number of snapshot save", ValueType::Number) \
    M(KeeperCreateRequest, "Number of create requests", ValueType::Number) \
    M(KeeperRemoveRequest, "Number of remove requests", ValueType::Number) \
    M(KeeperSetRequest, "Number of set requests", ValueType::Number) \
    M(KeeperReconfigRequest, "Number of reconfig requests", ValueType::Number) \
    M(KeeperCheckRequest, "Number of check requests", ValueType::Number) \
    M(KeeperMultiRequest, "Number of multi requests", ValueType::Number) \
    M(KeeperMultiReadRequest, "Number of multi read requests", ValueType::Number) \
    M(KeeperGetRequest, "Number of get requests", ValueType::Number) \
    M(KeeperListRequest, "Number of list requests", ValueType::Number) \
    M(KeeperExistsRequest, "Number of exists requests", ValueType::Number) \
    \
    M(OverflowBreak, "Number of times, data processing was cancelled by query complexity limitation with setting '*_overflow_mode' = 'break' and the result is incomplete.", ValueType::Number) \
    M(OverflowThrow, "Number of times, data processing was cancelled by query complexity limitation with setting '*_overflow_mode' = 'throw' and exception was thrown.", ValueType::Number) \
    M(OverflowAny, "Number of times approximate GROUP BY was in effect: when aggregation was performed only on top of first 'max_rows_to_group_by' unique keys and other keys were ignored due to 'group_by_overflow_mode' = 'any'.", ValueType::Number) \
    \
    M(S3QueueSetFileProcessingMicroseconds, "Time spent to set file as processing", ValueType::Microseconds) \
    M(S3QueueSetFileProcessedMicroseconds, "Time spent to set file as processed", ValueType::Microseconds) \
    M(S3QueueSetFileFailedMicroseconds, "Time spent to set file as failed", ValueType::Microseconds) \
    M(ObjectStorageQueueFailedFiles, "Number of files which failed to be processed", ValueType::Number) \
    M(ObjectStorageQueueProcessedFiles, "Number of files which were processed", ValueType::Number) \
    M(ObjectStorageQueueCleanupMaxSetSizeOrTTLMicroseconds, "Time spent to set file as failed", ValueType::Microseconds) \
    M(ObjectStorageQueuePullMicroseconds, "Time spent to read file data", ValueType::Microseconds) \
    M(ObjectStorageQueueLockLocalFileStatusesMicroseconds, "Time spent to lock local file statuses", ValueType::Microseconds) \
    M(ObjectStorageQueueFailedToBatchSetProcessing, "Number of times batched set processing request failed", ValueType::Number) \
    M(ObjectStorageQueueTrySetProcessingRequests, "The number of times we tried to make set processing request", ValueType::Number) \
    M(ObjectStorageQueueTrySetProcessingSucceeded, "The number of times we successfully set file as processing", ValueType::Number) \
    M(ObjectStorageQueueTrySetProcessingFailed, "The number of times we unsuccessfully set file as processing", ValueType::Number) \
    M(ObjectStorageQueueListedFiles, "Number of listed files in StorageS3(Azure)Queue", ValueType::Number) \
    M(ObjectStorageQueueFilteredFiles, "Number of filtered files in StorageS3(Azure)Queue", ValueType::Number) \
    M(ObjectStorageQueueReadFiles, "Number of read files (not equal to the number of actually inserted files)", ValueType::Number) \
    M(ObjectStorageQueueReadRows, "Number of read rows (not equal to the number of actually inserted rows)", ValueType::Number) \
    M(ObjectStorageQueueReadBytes, "Number of read bytes (not equal to the number of actually inserted bytes)", ValueType::Number) \
    M(ObjectStorageQueueExceptionsDuringRead, "Number of exceptions during read in S3(Azure)Queue", ValueType::Number) \
    M(ObjectStorageQueueExceptionsDuringInsert, "Number of exceptions during insert in S3(Azure)Queue", ValueType::Number) \
    M(ObjectStorageQueueRemovedObjects, "Number of objects removed as part of after_processing = delete", ValueType::Number) \
    M(ObjectStorageQueueInsertIterations, "Number of insert iterations", ValueType::Number) \
    M(ObjectStorageQueueCommitRequests, "Number of keeper requests to commit files as either failed or processed", ValueType::Number) \
    M(ObjectStorageQueueSuccessfulCommits, "Number of successful keeper commits", ValueType::Number) \
    M(ObjectStorageQueueUnsuccessfulCommits, "Number of unsuccessful keeper commits", ValueType::Number) \
    M(ObjectStorageQueueCancelledFiles, "Number cancelled files in StorageS3(Azure)Queue", ValueType::Number) \
    M(ObjectStorageQueueProcessedRows, "Number of processed rows in StorageS3(Azure)Queue", ValueType::Number) \
    \
    M(ServerStartupMilliseconds, "Time elapsed from starting server to listening to sockets in milliseconds", ValueType::Milliseconds) \
    M(IOUringSQEsSubmitted, "Total number of io_uring SQEs submitted", ValueType::Number) \
    M(IOUringSQEsResubmitsAsync, "Total number of asynchronous io_uring SQE resubmits performed", ValueType::Number) \
    M(IOUringSQEsResubmitsSync, "Total number of synchronous io_uring SQE resubmits performed", ValueType::Number) \
    M(IOUringCQEsCompleted, "Total number of successfully completed io_uring CQEs", ValueType::Number) \
    M(IOUringCQEsFailed, "Total number of completed io_uring CQEs with failures", ValueType::Number) \
    \
    M(BackupsOpenedForRead, "Number of backups opened for reading", ValueType::Number) \
    M(BackupsOpenedForWrite, "Number of backups opened for writing", ValueType::Number) \
    M(BackupsOpenedForUnlock, "Number of backups opened for unlocking", ValueType::Number) \
    M(BackupReadMetadataMicroseconds, "Time spent reading backup metadata from .backup file", ValueType::Microseconds) \
    M(BackupWriteMetadataMicroseconds, "Time spent writing backup metadata to .backup file", ValueType::Microseconds) \
    M(BackupEntriesCollectorMicroseconds, "Time spent making backup entries", ValueType::Microseconds) \
    M(BackupEntriesCollectorForTablesDataMicroseconds, "Time spent making backup entries for tables data", ValueType::Microseconds) \
    M(BackupEntriesCollectorRunPostTasksMicroseconds, "Time spent running post tasks after making backup entries", ValueType::Microseconds) \
    M(BackupPreparingFileInfosMicroseconds, "Time spent preparing file infos for backup entries", ValueType::Microseconds) \
    M(BackupReadLocalFilesToCalculateChecksums, "Number of files read locally to calculate checksums for backup entries", ValueType::Number) \
    M(BackupReadLocalBytesToCalculateChecksums, "Total size of files read locally to calculate checksums for backup entries", ValueType::Number) \
    M(BackupReadRemoteFilesToCalculateChecksums, "Number of files read from remote disks to calculate checksums for backup entries", ValueType::Number) \
    M(BackupReadRemoteBytesToCalculateChecksums, "Total size of files read from remote disks to calculate checksums for backup entries", ValueType::Number) \
    M(BackupLockFileReads, "How many times the '.lock' file was read while making backup", ValueType::Number) \
    M(RestorePartsSkippedFiles, "Number of files skipped while restoring parts", ValueType::Number) \
    M(RestorePartsSkippedBytes, "Total size of files skipped while restoring parts", ValueType::Number) \
    \
    M(ReadTaskRequestsReceived, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the initiator server side.", ValueType::Number) \
    M(MergeTreeReadTaskRequestsReceived, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the initiator server side.", ValueType::Number) \
    \
    M(ReadTaskRequestsSent, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the remote server side.", ValueType::Number) \
    M(MergeTreeReadTaskRequestsSent, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the remote server side.", ValueType::Number) \
    M(MergeTreeAllRangesAnnouncementsSent, "The number of announcements sent from the remote server to the initiator server about the set of data parts (for MergeTree tables). Measured on the remote server side.", ValueType::Number) \
    M(ReadTaskRequestsSentElapsedMicroseconds, "Time spent in callbacks requested from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the remote server side.", ValueType::Microseconds) \
    M(MergeTreeReadTaskRequestsSentElapsedMicroseconds, "Time spent in callbacks requested from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the remote server side.", ValueType::Microseconds) \
    M(MergeTreeAllRangesAnnouncementsSentElapsedMicroseconds, "Time spent in sending the announcement from the remote server to the initiator server about the set of data parts (for MergeTree tables). Measured on the remote server side.", ValueType::Microseconds) \
    M(MergerMutatorsGetPartsForMergeElapsedMicroseconds, "Time spent to take data parts snapshot to build ranges from them.", ValueType::Microseconds) \
    M(MergerMutatorPrepareRangesForMergeElapsedMicroseconds, "Time spent to prepare parts ranges which can be merged according to merge predicate.", ValueType::Microseconds) \
    M(MergerMutatorSelectPartsForMergeElapsedMicroseconds, "Time spent to select parts from ranges which can be merged.", ValueType::Microseconds) \
    M(MergerMutatorRangesForMergeCount, "Amount of candidate ranges for merge", ValueType::Number) \
    M(MergerMutatorPartsInRangesForMergeCount, "Amount of candidate parts for merge", ValueType::Number) \
    M(MergerMutatorSelectRangePartsCount, "Amount of parts in selected range for merge", ValueType::Number) \
    \
    M(ConnectionPoolIsFullMicroseconds, "Total time spent waiting for a slot in connection pool.", ValueType::Microseconds) \
    M(AsyncLoaderWaitMicroseconds, "Total time a query was waiting for async loader jobs.", ValueType::Microseconds) \
    \
    M(DistrCacheServerSwitches, "Distributed Cache read buffer event. Number of server switches between distributed cache servers in read/write-through cache", ValueType::Number) \
    M(DistrCacheReadMicroseconds, "Distributed Cache read buffer event. Time spent reading from distributed cache", ValueType::Microseconds) \
    M(DistrCacheFallbackReadMicroseconds, "Distributed Cache read buffer event. Time spend reading from fallback buffer instead of distributed cache", ValueType::Microseconds) \
    M(DistrCachePrecomputeRangesMicroseconds, "Distributed Cache read buffer event. Time spent to precompute read ranges", ValueType::Microseconds) \
    M(DistrCacheNextImplMicroseconds, "Distributed Cache read buffer event. Time spend in ReadBufferFromDistributedCache::nextImpl", ValueType::Microseconds) \
    M(DistrCacheStartRangeMicroseconds, "Distributed Cache read buffer event. Time spent to start a new read range with distributed cache", ValueType::Microseconds) \
    M(DistrCacheRangeChange, "Distributed Cache read buffer event. Number of times we changed read range because of seek/last_position change", ValueType::Number) \
    M(DistrCacheRangeResetBackward, "Distributed Cache read buffer event. Number of times we reset read range because of seek/last_position change", ValueType::Number) \
    M(DistrCacheRangeResetForward, "Distributed Cache read buffer event. Number of times we reset read range because of seek/last_position change", ValueType::Number) \
    M(DistrCacheReconnectsAfterTimeout, "Distributed Cache read buffer event. The number of reconnects after timeout", ValueType::Number) \
    M(DistrCacheServerUpdates, "Distributed Cache read buffer event. The number of server updates because server is not longer registered in keeper", ValueType::Number) \
    \
    M(DistrCacheGetResponseMicroseconds, "Distributed Cache client event. Time spend to wait for response from distributed cache", ValueType::Microseconds) \
    M(DistrCacheReadErrors, "Distributed Cache client event. Number of distributed cache errors during read", ValueType::Number) \
    M(DistrCacheMakeRequestErrors, "Distributed Cache client event. Number of distributed cache errors when making a request", ValueType::Number) \
    M(DistrCacheReceiveResponseErrors, "Distributed Cache client event. Number of distributed cache errors when receiving response a request", ValueType::Number) \
    \
    M(DistrCachePackets, "Distributed Cache client event. Total number of packets received from distributed cache", ValueType::Number) \
    M(DistrCacheDataPacketsBytes, "Distributed Cache client event. The number of bytes in Data packets which were not ignored", ValueType::Bytes) \
    M(DistrCacheUnusedPackets, "Distributed Cache client event. Number of skipped unused packets from distributed cache", ValueType::Number) \
    M(DistrCacheUnusedDataPacketsBytes, "Distributed Cache client event. The number of bytes in Data packets which were ignored", ValueType::Bytes) \
    M(DistrCacheUnusedPacketsBufferAllocations, "Distributed Cache client event. The number of extra buffer allocations in case we could not reuse existing buffer", ValueType::Number) \
    \
    M(DistrCacheLockRegistryMicroseconds, "Distributed Cache registry event. Time spent to take DistributedCacheRegistry lock", ValueType::Microseconds) \
    M(DistrCacheRegistryUpdateMicroseconds, "Distributed Cache registry event. Time spent updating distributed cache registry", ValueType::Microseconds) \
    M(DistrCacheRegistryUpdates, "Distributed Cache registry event. Number of distributed cache registry updates", ValueType::Number) \
    M(DistrCacheHashRingRebuilds, "Distributed Cache registry event. Number of distributed cache hash ring rebuilds", ValueType::Number) \
    \
    M(DistrCacheReadBytesFromFallbackBuffer, "Distributed Cache read buffer event. Bytes read from fallback buffer", ValueType::Number) \
    \
    M(DistrCacheOpenedConnections, "Distributed Cache connection event. The number of open connections to distributed cache", ValueType::Number) \
    M(DistrCacheReusedConnections, "Distributed Cache connection event. The number of reused connections to distributed cache", ValueType::Number) \
    M(DistrCacheOpenedConnectionsBypassingPool, "Distributed Cache connection event. The number of open connections to distributed cache bypassing pool", ValueType::Number) \
    M(DistrCacheConnectMicroseconds, "Distributed Cache connection event. The time spent to connect to distributed cache", ValueType::Microseconds) \
    M(DistrCacheConnectAttempts, "Distributed Cache connection event. The number of connection attempts to distributed cache", ValueType::Number) \
    M(DistrCacheGetClientMicroseconds, "Distributed Cache connection event. Time spent getting client for distributed cache", ValueType::Microseconds) \
    \
    M(DistrCacheServerProcessRequestMicroseconds, "Distributed Cache server event. Time spent processing request on DistributedCache server side", ValueType::Microseconds) \
    M(DistrCacheServerStartRequestPackets, "Distributed Cache server event. Number of StartRequest packets in DistributedCacheServer", ValueType::Number) \
    M(DistrCacheServerContinueRequestPackets, "Distributed Cache server event. Number of ContinueRequest packets in DistributedCacheServer", ValueType::Number) \
    M(DistrCacheServerEndRequestPackets, "Distributed Cache server event. Number of EndRequest packets in DistributedCacheServer", ValueType::Number) \
    M(DistrCacheServerReceivedCredentialsRefreshPackets, "Distributed Cache server event. Number of RefreshCredentials client packets in DistributedCacheServer", ValueType::Number) \
    M(DistrCacheServerAckRequestPackets, "Distributed Cache server event. Number of AckRequest packets in DistributedCacheServer", ValueType::Number) \
    M(DistrCacheServerNewS3CachedClients, "Distributed Cache server event. The number of new cached s3 clients", ValueType::Number) \
    M(DistrCacheServerReusedS3CachedClients, "Distributed Cache server event. The number of reused cached s3 clients", ValueType::Number) \
    M(DistrCacheServerCredentialsRefresh, "Distributed Cache server event. The number of expired credentials were refreshed", ValueType::Number) \
    \
    M(LogTest, "Number of log messages with level Test", ValueType::Number) \
    M(LogTrace, "Number of log messages with level Trace", ValueType::Number) \
    M(LogDebug, "Number of log messages with level Debug", ValueType::Number) \
    M(LogInfo, "Number of log messages with level Info", ValueType::Number) \
    M(LogWarning, "Number of log messages with level Warning", ValueType::Number) \
    M(LogError, "Number of log messages with level Error", ValueType::Number) \
    M(LogFatal, "Number of log messages with level Fatal", ValueType::Number) \
    M(LoggerElapsedNanoseconds, "Cumulative time spend in logging", ValueType::Nanoseconds) \
    \
    M(InterfaceHTTPSendBytes, "Number of bytes sent through HTTP interfaces", ValueType::Bytes) \
    M(InterfaceHTTPReceiveBytes, "Number of bytes received through HTTP interfaces", ValueType::Bytes) \
    M(InterfaceNativeSendBytes, "Number of bytes sent through native interfaces", ValueType::Bytes) \
    M(InterfaceNativeReceiveBytes, "Number of bytes received through native interfaces", ValueType::Bytes) \
    M(InterfacePrometheusSendBytes, "Number of bytes sent through Prometheus interfaces", ValueType::Bytes) \
    M(InterfacePrometheusReceiveBytes, "Number of bytes received through Prometheus interfaces", ValueType::Bytes) \
    M(InterfaceInterserverSendBytes, "Number of bytes sent through interserver interfaces", ValueType::Bytes) \
    M(InterfaceInterserverReceiveBytes, "Number of bytes received through interserver interfaces", ValueType::Bytes) \
    M(InterfaceMySQLSendBytes, "Number of bytes sent through MySQL interfaces", ValueType::Bytes) \
    M(InterfaceMySQLReceiveBytes, "Number of bytes received through MySQL interfaces", ValueType::Bytes) \
    M(InterfacePostgreSQLSendBytes, "Number of bytes sent through PostgreSQL interfaces", ValueType::Bytes) \
    M(InterfacePostgreSQLReceiveBytes, "Number of bytes received through PostgreSQL interfaces", ValueType::Bytes) \
    \
    M(ParallelReplicasUsedCount, "Number of replicas used to execute a query with task-based parallel replicas", ValueType::Number) \
    M(ParallelReplicasAvailableCount, "Number of replicas available to execute a query with task-based parallel replicas", ValueType::Number) \
    M(ParallelReplicasUnavailableCount, "Number of replicas which was chosen, but found to be unavailable during query execution with task-based parallel replicas", ValueType::Number) \
    \
    M(SharedMergeTreeVirtualPartsUpdates, "Virtual parts update count", ValueType::Number) \
    M(SharedMergeTreeVirtualPartsUpdatesByLeader, "Virtual parts updates by leader", ValueType::Number) \
    M(SharedMergeTreeVirtualPartsUpdateMicroseconds, "Virtual parts update microseconds", ValueType::Microseconds) \
    M(SharedMergeTreeVirtualPartsUpdatesFromZooKeeper, "Virtual parts updates count from ZooKeeper", ValueType::Number) \
    M(SharedMergeTreeVirtualPartsUpdatesFromZooKeeperMicroseconds, "Virtual parts updates from ZooKeeper microseconds", ValueType::Microseconds) \
    M(SharedMergeTreeVirtualPartsUpdatesPeerNotFound, "Virtual updates from peer failed because no one found", ValueType::Number) \
    M(SharedMergeTreeVirtualPartsUpdatesFromPeer, "Virtual parts updates count from peer", ValueType::Number) \
    M(SharedMergeTreeVirtualPartsUpdatesFromPeerMicroseconds, "Virtual parts updates from peer microseconds", ValueType::Microseconds) \
    M(SharedMergeTreeVirtualPartsUpdatesForMergesOrStatus, "Virtual parts updates from non-default background job", ValueType::Number) \
    M(SharedMergeTreeVirtualPartsUpdatesLeaderFailedElection, "Virtual parts updates leader election failed", ValueType::Number) \
    M(SharedMergeTreeVirtualPartsUpdatesLeaderSuccessfulElection, "Virtual parts updates leader election successful", ValueType::Number) \
    M(SharedMergeTreeMergeMutationAssignmentAttempt, "How many times we tried to assign merge or mutation", ValueType::Number) \
    M(SharedMergeTreeMergeMutationAssignmentFailedWithNothingToDo, "How many times we tried to assign merge or mutation and failed because nothing to merge", ValueType::Number) \
    M(SharedMergeTreeMergeMutationAssignmentFailedWithConflict, "How many times we tried to assign merge or mutation and failed because of conflict in Keeper", ValueType::Number) \
    M(SharedMergeTreeMergeMutationAssignmentSuccessful, "How many times we tried to assign merge or mutation", ValueType::Number) \
    M(SharedMergeTreeMergePartsMovedToOudated, "How many parts moved to oudated directory", ValueType::Number) \
    M(SharedMergeTreeMergePartsMovedToCondemned, "How many parts moved to condemned directory", ValueType::Number) \
    M(SharedMergeTreeOutdatedPartsConfirmationRequest, "How many ZooKeeper requests were used to config outdated parts", ValueType::Number) \
    M(SharedMergeTreeOutdatedPartsConfirmationInvocations, "How many invocations were made to confirm outdated parts", ValueType::Number) \
    M(SharedMergeTreeOutdatedPartsHTTPRequest, "How many HTTP requests were send to confirm outdated parts", ValueType::Number) \
    M(SharedMergeTreeOutdatedPartsHTTPResponse, "How many HTTP responses were send to confirm outdated parts", ValueType::Number) \
    M(SharedMergeTreeCondemnedPartsKillRequest, "How many ZooKeeper requests were used to remove condemned parts", ValueType::Number) \
    M(SharedMergeTreeCondemnedPartsLockConfict, "How many times we failed to acquite lock because of conflict", ValueType::Number) \
    M(SharedMergeTreeCondemnedPartsRemoved, "How many condemned parts were removed", ValueType::Number) \
    M(SharedMergeTreeMergeSelectingTaskMicroseconds, "Merge selecting task microseconds for SMT", ValueType::Number) \
    M(SharedMergeTreeOptimizeAsync, "Asynchronous OPTIMIZE queries executed", ValueType::Number) \
    M(SharedMergeTreeOptimizeSync, "Synchronous OPTIMIZE queries executed", ValueType::Number) \
    M(SharedMergeTreeScheduleDataProcessingJob, "How many times scheduleDataProcessingJob called/", ValueType::Number) \
    M(SharedMergeTreeScheduleDataProcessingJobNothingToScheduled, "How many times scheduleDataProcessingJob called but nothing to do", ValueType::Number) \
    M(SharedMergeTreeScheduleDataProcessingJobMicroseconds, "scheduleDataProcessingJob execute time", ValueType::Number) \
    M(SharedMergeTreeHandleBlockingParts, "How many blocking parts to handle in scheduleDataProcessingJob", ValueType::Number) \
    M(SharedMergeTreeHandleBlockingPartsMicroseconds, "Time of handling blocking parts in scheduleDataProcessingJob ", ValueType::Number) \
    M(SharedMergeTreeHandleFetchPartsMicroseconds, "Time of handling fetched parts in scheduleDataProcessingJob", ValueType::Number) \
    M(SharedMergeTreeHandleOutdatedParts, "How many outdated parts to handle in scheduleDataProcessingJob", ValueType::Number) \
    M(SharedMergeTreeHandleOutdatedPartsMicroseconds, "Time of handling outdated parts in scheduleDataProcessingJob", ValueType::Number) \
    M(SharedMergeTreeGetPartsBatchToLoadMicroseconds, "Time of getPartsBatchToLoad in scheduleDataProcessingJob", ValueType::Number) \
    M(SharedMergeTreeTryUpdateDiskMetadataCacheForPartMicroseconds, "Time of tryUpdateDiskMetadataCacheForPart in scheduleDataProcessingJob", ValueType::Number) \
    M(SharedMergeTreeLoadChecksumAndIndexesMicroseconds, "Time of loadColumnsChecksumsIndexes only for SharedMergeTree", ValueType::Number)                                                                                                                                                                                                             \
    \
    M(SharedMergeTreeDataPartsFetchAttempt, "How many times we tried to fetch data parts", ValueType::Number) \
    M(SharedMergeTreeDataPartsFetchFromPeer, "How many times we fetch data parts from peer", ValueType::Number) \
    M(SharedMergeTreeDataPartsFetchFromPeerMicroseconds, "Data parts fetch from peer microseconds", ValueType::Number) \
    M(SharedMergeTreeDataPartsFetchFromS3, "How many times we fetch data parts from S3", ValueType::Number) \
    \
    M(KeeperLogsEntryReadFromLatestCache, "Number of log entries in Keeper being read from latest logs cache", ValueType::Number) \
    M(KeeperLogsEntryReadFromCommitCache, "Number of log entries in Keeper being read from commit logs cache", ValueType::Number) \
    M(KeeperLogsEntryReadFromFile, "Number of log entries in Keeper being read directly from the changelog file", ValueType::Number) \
    M(KeeperLogsPrefetchedEntries, "Number of log entries in Keeper being prefetched from the changelog file", ValueType::Number) \
    \
    M(StorageConnectionsCreated, "Number of created connections for storages", ValueType::Number) \
    M(StorageConnectionsReused, "Number of reused connections for storages", ValueType::Number) \
    M(StorageConnectionsReset, "Number of reset connections for storages", ValueType::Number) \
    M(StorageConnectionsPreserved, "Number of preserved connections for storages", ValueType::Number) \
    M(StorageConnectionsExpired, "Number of expired connections for storages", ValueType::Number) \
    M(StorageConnectionsErrors, "Number of cases when creation of a connection for storage is failed", ValueType::Number) \
    M(StorageConnectionsElapsedMicroseconds, "Total time spend on creating connections for storages", ValueType::Microseconds) \
    \
    M(DiskConnectionsCreated, "Number of created connections for disk", ValueType::Number) \
    M(DiskConnectionsReused, "Number of reused connections for disk", ValueType::Number) \
    M(DiskConnectionsReset, "Number of reset connections for disk", ValueType::Number) \
    M(DiskConnectionsPreserved, "Number of preserved connections for disk", ValueType::Number) \
    M(DiskConnectionsExpired, "Number of expired connections for disk", ValueType::Number) \
    M(DiskConnectionsErrors, "Number of cases when creation of a connection for disk is failed", ValueType::Number) \
    M(DiskConnectionsElapsedMicroseconds, "Total time spend on creating connections for disk", ValueType::Microseconds) \
    \
    M(HTTPConnectionsCreated, "Number of created client HTTP connections", ValueType::Number) \
    M(HTTPConnectionsReused, "Number of reused client HTTP connections", ValueType::Number) \
    M(HTTPConnectionsReset, "Number of reset client HTTP connections", ValueType::Number) \
    M(HTTPConnectionsPreserved, "Number of preserved client HTTP connections", ValueType::Number) \
    M(HTTPConnectionsExpired, "Number of expired client HTTP connections", ValueType::Number) \
    M(HTTPConnectionsErrors, "Number of cases when creation of a client HTTP connection failed", ValueType::Number) \
    M(HTTPConnectionsElapsedMicroseconds, "Total time spend on creating client HTTP connections", ValueType::Microseconds) \
    \
    M(HTTPServerConnectionsCreated, "Number of created server HTTP connections", ValueType::Number) \
    M(HTTPServerConnectionsReused, "Number of reused server HTTP connections", ValueType::Number) \
    M(HTTPServerConnectionsPreserved, "Number of preserved server HTTP connections. Connection kept alive successfully", ValueType::Number) \
    M(HTTPServerConnectionsExpired, "Number of expired server HTTP connections.", ValueType::Number) \
    M(HTTPServerConnectionsClosed, "Number of closed server HTTP connections. Keep alive has not been negotiated", ValueType::Number) \
    M(HTTPServerConnectionsReset, "Number of reset server HTTP connections. Server closes connection", ValueType::Number) \
    \
    M(AddressesDiscovered, "Total count of new addresses in DNS resolve results for HTTP connections", ValueType::Number) \
    M(AddressesExpired, "Total count of expired addresses which is no longer presented in DNS resolve results for HTTP connections", ValueType::Number) \
    M(AddressesMarkedAsFailed, "Total count of addresses which have been marked as faulty due to connection errors for HTTP connections", ValueType::Number) \
    \
    M(ReadWriteBufferFromHTTPRequestsSent, "Number of HTTP requests sent by ReadWriteBufferFromHTTP", ValueType::Number) \
    M(ReadWriteBufferFromHTTPBytes, "Total size of payload bytes received and sent by ReadWriteBufferFromHTTP. Doesn't include HTTP headers.", ValueType::Bytes) \
    \
    M(WriteBufferFromHTTPRequestsSent, "Number of HTTP requests sent by WriteBufferFromHTTP", ValueType::Number) \
    M(WriteBufferFromHTTPBytes, "Total size of payload bytes received and sent by WriteBufferFromHTTP. Doesn't include HTTP headers.", ValueType::Bytes) \
    \
    M(ConcurrencyControlSlotsGranted, "Number of CPU slot granted according to guarantee of 1 thread per query and for queries with setting 'use_concurrency_control' = 0", ValueType::Number) \
    M(ConcurrencyControlSlotsDelayed, "Number of CPU slot not granted initially and required to wait for a free CPU slot", ValueType::Number) \
    M(ConcurrencyControlSlotsAcquired, "Total number of CPU slots acquired", ValueType::Number) \
    M(ConcurrencyControlSlotsAcquiredNonCompeting, "Total number of noncompeting CPU slot acquired", ValueType::Number) \
    M(ConcurrencyControlQueriesDelayed, "Total number of CPU slot allocations (queries) that were required to wait for slots to upscale", ValueType::Number) \
    M(ConcurrencyControlWaitMicroseconds, "Total time a query was waiting on resource requests for CPU slots", ValueType::Microseconds) \
    \
    M(ConcurrentQuerySlotsAcquired, "Total number of query slots acquired", ValueType::Number) \
    M(ConcurrentQueryWaitMicroseconds, "Total time a query was waiting for a query slots", ValueType::Microseconds) \
    \
    M(CoordinatedMergesMergeCoordinatorUpdateCount, "Total number of merge coordinator updates", ValueType::Number) \
    M(CoordinatedMergesMergeCoordinatorUpdateMicroseconds, "Total time spend on updating merge coordinator state", ValueType::Microseconds) \
    M(CoordinatedMergesMergeCoordinatorFetchMetadataMicroseconds, "Total time spend on fetching fresh metadata inside merge coordinator", ValueType::Microseconds) \
    M(CoordinatedMergesMergeCoordinatorFilterMicroseconds, "Total time spend on filtering prepared merges inside merge coordinator", ValueType::Microseconds) \
    M(CoordinatedMergesMergeCoordinatorSelectMergesMicroseconds, "Total time spend on finding merge using merge selectors inside merge coordinator", ValueType::Microseconds) \
    M(CoordinatedMergesMergeCoordinatorLockStateForShareCount, "Total number of for share captures of coordinator state lock", ValueType::Number) \
    M(CoordinatedMergesMergeCoordinatorLockStateExclusivelyCount, "Total number of exclusive captures of coordinator state lock", ValueType::Number) \
    M(CoordinatedMergesMergeCoordinatorLockStateForShareMicroseconds, "Total time spend on locking coordinator state mutex for share", ValueType::Microseconds) \
    M(CoordinatedMergesMergeCoordinatorLockStateExclusivelyMicroseconds, "Total time spend on locking coordinator state mutex exclusively", ValueType::Microseconds) \
    M(CoordinatedMergesMergeWorkerUpdateCount, "Total number merge worker updates", ValueType::Number) \
    M(CoordinatedMergesMergeWorkerUpdateMicroseconds, "Total time spend on updating local state of assigned merges on worker", ValueType::Microseconds) \
    M(CoordinatedMergesMergeAssignmentRequest, "Total number of merge assignment requests", ValueType::Number) \
    M(CoordinatedMergesMergeAssignmentResponse, "Total number of merge assignment requests", ValueType::Number) \
    M(CoordinatedMergesMergeAssignmentRequestMicroseconds, "Total time spend in merge assignment client", ValueType::Microseconds) \
    M(CoordinatedMergesMergeAssignmentResponseMicroseconds, "Total time spend in merge assignment handler", ValueType::Microseconds) \
    \
    M(SharedDatabaseCatalogFailedToApplyState, "Number of failures to apply new state in SharedDatabaseCatalog", ValueType::Number) \
    M(SharedDatabaseCatalogStateApplicationMicroseconds, "Total time spend on application of new state in SharedDatabaseCatalog", ValueType::Microseconds) \
    \
    M(GWPAsanAllocateSuccess, "Number of successful allocations done by GWPAsan", ValueType::Number) \
    M(GWPAsanAllocateFailed, "Number of failed allocations done by GWPAsan (i.e. filled pool)", ValueType::Number) \
    M(GWPAsanFree, "Number of free operations done by GWPAsan", ValueType::Number) \
    \
    M(MemoryWorkerRun, "Number of runs done by MemoryWorker in background", ValueType::Number) \
    M(MemoryWorkerRunElapsedMicroseconds, "Total time spent by MemoryWorker for background work", ValueType::Microseconds) \
    \
    M(ParquetFetchWaitTimeMicroseconds, "Time of waiting fetching parquet data", ValueType::Microseconds) \
    M(ParquetReadRowGroups, "The total number of row groups read from parquet data", ValueType::Number) \
    M(ParquetPrunedRowGroups, "The total number of row groups pruned from parquet data", ValueType::Number) \
    M(FilterTransformPassedRows, "Number of rows that passed the filter in the query", ValueType::Number) \
    M(FilterTransformPassedBytes, "Number of bytes that passed the filter in the query", ValueType::Bytes) \
    M(QueryPreempted, "How many times tasks are paused and waiting due to 'priority' setting", ValueType::Number) \
    M(IndexBinarySearchAlgorithm, "Number of times the binary search algorithm is used over the index marks", ValueType::Number) \
    M(IndexGenericExclusionSearchAlgorithm, "Number of times the generic exclusion search algorithm is used over the index marks", ValueType::Number) \
    M(ParallelReplicasQueryCount, "Number of (sub)queries executed using parallel replicas during a query execution", ValueType::Number) \
    M(DistributedConnectionReconnectCount, "Number of reconnects to other servers done during distributed query execution. It can happen when a stale connection has been acquired from connection pool", ValueType::Number) \
    \
    M(RefreshableViewRefreshSuccess, "How many times refreshable materialized views refreshed", ValueType::Number) \
    M(RefreshableViewRefreshFailed, "How many times refreshable materialized views failed to refresh", ValueType::Number) \
    M(RefreshableViewSyncReplicaSuccess, "How many times a SELECT from refreshable materialized view did an implicit SYNC REPLICA", ValueType::Number) \
    M(RefreshableViewSyncReplicaRetry, "How many times a SELECT from refreshable materialized view failed and retried an implicit SYNC REPLICA", ValueType::Number) \
    M(RefreshableViewLockTableRetry, "How many times a SELECT from refreshable materialized view had to switch to a new table because the old table was dropped", ValueType::Number) \


#ifdef APPLY_FOR_EXTERNAL_EVENTS
    #define APPLY_FOR_EVENTS(M) APPLY_FOR_BUILTIN_EVENTS(M) APPLY_FOR_EXTERNAL_EVENTS(M)
#else
    #define APPLY_FOR_EVENTS(M) APPLY_FOR_BUILTIN_EVENTS(M)
#endif

namespace DB::ErrorCodes
{
    extern const int SERVER_OVERLOADED;
}

namespace ProfileEvents
{

#define M(NAME, DOCUMENTATION, VALUE_TYPE) extern const Event NAME = Event(__COUNTER__);
    APPLY_FOR_EVENTS(M)
#undef M
constexpr Event END = Event(__COUNTER__);

/// Global variable, initialized by zeros.
static Counter global_counters_array[END] {};
/// Initialize global counters statically
Counters global_counters(global_counters_array);

const Event Counters::num_counters = END;


Timer::Timer(Counters & counters_, Event timer_event_, Resolution resolution_)
    : counters(counters_), timer_event(timer_event_), resolution(resolution_)
{
}

Timer::Timer(Counters & counters_, Event timer_event_, Event counter_event, Resolution resolution_)
    : Timer(counters_, timer_event_, resolution_)
{
    counters.increment(counter_event);
}

UInt64 Timer::get()
{
    return watch.elapsedNanoseconds() / static_cast<UInt64>(resolution);
}

void Timer::end()
{
    counters.increment(timer_event, get());
    watch.reset();
}

Counters::Counters(VariableContext level_, Counters * parent_)
    : counters_holder(new Counter[num_counters] {}),
      parent(parent_),
      level(level_)
{
    counters = counters_holder.get();
}

Counters::Counters(Counters && src) noexcept
    : counters(std::exchange(src.counters, nullptr))
    , counters_holder(std::move(src.counters_holder))
    , parent(src.parent.exchange(nullptr))
    , trace_profile_events(src.trace_profile_events)
    , level(src.level)
{
}

void Counters::resetCounters()
{
    if (counters)
    {
        for (Event i = Event(0); i < num_counters; ++i)
            counters[i].store(0, std::memory_order_relaxed);
    }
}

void Counters::reset()
{
    setParent(nullptr);
    resetCounters();
}

Counters::Snapshot::Snapshot()
    : counters_holder(new Count[num_counters] {})
{}

Counters::Snapshot Counters::getPartiallyAtomicSnapshot() const
{
    Snapshot res;
    for (Event i = Event(0); i < num_counters; ++i)
        res.counters_holder[i] = counters[i].load(std::memory_order_relaxed);
    return res;
}

const char * getName(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION, VALUE_TYPE) #NAME,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

const char * getDocumentation(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION, VALUE_TYPE) DOCUMENTATION,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

ValueType getValueType(Event event)
{
    static ValueType strings[] =
    {
    #define M(NAME, DOCUMENTATION, VALUE_TYPE) VALUE_TYPE,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

Event end() { return END; }

bool checkCPUOverload(Int64 os_cpu_busy_time_threshold, double min_ratio, double max_ratio, bool should_throw)
{
    if ((max_ratio <= 0.0) || (max_ratio <= min_ratio))
        return false;
    double cpu_load = global_counters.getCPUOverload(os_cpu_busy_time_threshold);

    if (cpu_load > DBL_EPSILON)
    {
        double current_ratio = std::min(std::max(min_ratio, cpu_load), max_ratio);
        double probability_to_throw = (max_ratio <= min_ratio) ? 0.0 : (current_ratio - min_ratio) / (max_ratio - min_ratio);

        const PreformattedMessage error_message = PreformattedMessage::create("CPU is overloaded, CPU is waiting for execution way more than executing, "
                "ratio of wait time (OSCPUWaitMicroseconds metric) to busy time (OSCPUVirtualTimeMicroseconds metric) is {}. "
                "Min ratio for error {}{}, max ratio for error {}{}, probability used to decide whether to {} {}.{}",
                current_ratio,
                should_throw ? "(min_os_cpu_wait_time_ratio_to_throw setting) " : "",
                min_ratio,
                should_throw ? "(max_os_cpu_wait_time_ratio_to_throw setting) " : "",
                max_ratio,
                should_throw ? "discard the query" : "drop the connection",
                probability_to_throw,
                should_throw ? " Consider reducing the number of queries or increase backoff between retries." : "");

        if (std::bernoulli_distribution server_overloaded(probability_to_throw); server_overloaded(thread_local_rng))
        {
            if (should_throw)
                throw DB::Exception(error_message, DB::ErrorCodes::SERVER_OVERLOADED);
            else
            {
                LOG_ERROR(getLogger("ProfileEvents"), error_message);
                return true;
            }
        }
    }

    return false;
}

void increment(Event event, Count amount)
{
    DB::CurrentThread::getProfileEvents().increment(event, amount);
}

void incrementNoTrace(Event event, Count amount)
{
    DB::CurrentThread::getProfileEvents().incrementNoTrace(event, amount);
}

double Counters::getCPUOverload(Int64 os_cpu_busy_time_threshold, bool reset)
{
    /// It's possible that we'll have slightly inconsistent values between wait time and busy time. But since we take the value of CPU wait time first,
    /// it should not affect the situation a lot. In the worst case scenario we will have a slightly lower CPU overload value than it should be, but it's fine.
    Int64 curr_cpu_wait_microseconds = counters[OSCPUWaitMicroseconds];
    Int64 curr_cpu_virtual_time_microseconds = counters[OSCPUVirtualTimeMicroseconds];

    Int64 os_cpu_wait_microseconds = curr_cpu_wait_microseconds - prev_cpu_wait_microseconds.load(std::memory_order_acquire);
    Int64 os_cpu_virtual_time_microseconds = curr_cpu_virtual_time_microseconds - prev_cpu_virtual_time_microseconds.load(std::memory_order_acquire);

    if (reset)
    {
        /// It's important to update wait time first, since the atomicity is not guaranteed for both counters at the same time.
        /// So in the worst case scenario, we'll update prev wait time first, which will result in an underestimated wait time and lower CPU overload value.
        prev_cpu_wait_microseconds.store(curr_cpu_wait_microseconds, std::memory_order_release);
        prev_cpu_virtual_time_microseconds.store(curr_cpu_virtual_time_microseconds, std::memory_order_release);
    }

    if (os_cpu_virtual_time_microseconds <= os_cpu_busy_time_threshold || os_cpu_wait_microseconds <= 0)
        return 0;

    return static_cast<double>(os_cpu_wait_microseconds) / os_cpu_virtual_time_microseconds;
}

void Counters::increment(Event event, Count amount)
{
    Counters * current = this;
    bool send_to_trace_log = false;

    do
    {
        send_to_trace_log |= current->trace_profile_events;
        current->counters[event].fetch_add(amount, std::memory_order_relaxed);
        current = current->parent;
    } while (current != nullptr);

    if (unlikely(send_to_trace_log))
        DB::TraceSender::send(DB::TraceType::ProfileEvent, StackTrace(), {.event = event, .increment = amount});
}

void Counters::incrementNoTrace(Event event, Count amount)
{
    Counters * current = this;
    do
    {
        current->counters[event].fetch_add(amount, std::memory_order_relaxed);
        current = current->parent;
    } while (current != nullptr);
}

void incrementForLogMessage(Poco::Message::Priority priority)
{
    switch (priority)
    {
        case Poco::Message::PRIO_TEST: increment(LogTest); break;
        case Poco::Message::PRIO_TRACE: increment(LogTrace); break;
        case Poco::Message::PRIO_DEBUG: increment(LogDebug); break;
        case Poco::Message::PRIO_INFORMATION: increment(LogInfo); break;
        case Poco::Message::PRIO_WARNING: increment(LogWarning); break;
        case Poco::Message::PRIO_ERROR: increment(LogError); break;
        case Poco::Message::PRIO_FATAL: increment(LogFatal); break;
        default: break;
    }
}

void incrementLoggerElapsedNanoseconds(UInt64 ns)
{
    increment(LoggerElapsedNanoseconds, ns);
}

CountersIncrement::CountersIncrement(Counters::Snapshot const & snapshot)
{
    init();
    memcpy(increment_holder.get(), snapshot.counters_holder.get(), Counters::num_counters * sizeof(Increment));
}

CountersIncrement::CountersIncrement(Counters::Snapshot const & after, Counters::Snapshot const & before)
{
    init();
    for (Event i = Event(0); i < Counters::num_counters; ++i)
        increment_holder[i] = static_cast<Increment>(after[i]) - static_cast<Increment>(before[i]);
}

void CountersIncrement::init()
{
    increment_holder = std::make_unique<Increment[]>(Counters::num_counters);
}

}

#undef APPLY_FOR_EVENTS
