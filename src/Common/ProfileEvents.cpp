#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <Common/TraceSender.h>


// clang-format off
/// Available events. Add something here as you wish.
/// If the event is generic (i.e. not server specific)
/// it should be also added to src/Coordination/KeeperConstant.cpp
#define APPLY_FOR_BUILTIN_EVENTS(M) \
    M(Query, "Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.", 0) \
    M(SelectQuery, "Same as Query, but only for SELECT queries.", 0) \
    M(InsertQuery, "Same as Query, but only for INSERT queries.", 0) \
    M(InitialQuery, "Same as Query, but only counts initial queries (see is_initial_query).", 0)\
    M(QueriesWithSubqueries, "Count queries with all subqueries", 0) \
    M(SelectQueriesWithSubqueries, "Count SELECT queries with all subqueries", 0) \
    M(InsertQueriesWithSubqueries, "Count INSERT queries with all subqueries", 0) \
    M(SelectQueriesWithPrimaryKeyUsage, "Count SELECT queries which use the primary key to evaluate the WHERE condition", 0) \
    M(AsyncInsertQuery, "Same as InsertQuery, but only for asynchronous INSERT queries.", 0) \
    M(AsyncInsertBytes, "Data size in bytes of asynchronous INSERT queries.", 0) \
    M(AsyncInsertRows, "Number of rows inserted by asynchronous INSERT queries.", 0) \
    M(AsyncInsertCacheHits, "Number of times a duplicate hash id has been found in asynchronous INSERT hash id cache.", 0) \
    M(FailedQuery, "Number of failed queries.", 0) \
    M(FailedSelectQuery, "Same as FailedQuery, but only for SELECT queries.", 0) \
    M(FailedInsertQuery, "Same as FailedQuery, but only for INSERT queries.", 0) \
    M(FailedAsyncInsertQuery, "Number of failed ASYNC INSERT queries.", 0) \
    M(QueryTimeMicroseconds, "Total time of all queries.", 0) \
    M(SelectQueryTimeMicroseconds, "Total time of SELECT queries.", 0) \
    M(InsertQueryTimeMicroseconds, "Total time of INSERT queries.", 0) \
    M(OtherQueryTimeMicroseconds, "Total time of queries that are not SELECT or INSERT.", 0) \
    M(FileOpen, "Number of files opened.", 0) \
    M(Seek, "Number of times the 'lseek' function was called.", 0) \
    M(ReadBufferFromFileDescriptorRead, "Number of reads (read/pread) from a file descriptor. Does not include sockets.", 0) \
    M(ReadBufferFromFileDescriptorReadFailed, "Number of times the read (read/pread) from a file descriptor have failed.", 0) \
    M(ReadBufferFromFileDescriptorReadBytes, "Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.", 0) \
    M(WriteBufferFromFileDescriptorWrite, "Number of writes (write/pwrite) to a file descriptor. Does not include sockets.", 0) \
    M(WriteBufferFromFileDescriptorWriteFailed, "Number of times the write (write/pwrite) to a file descriptor have failed.", 0) \
    M(WriteBufferFromFileDescriptorWriteBytes, "Number of bytes written to file descriptors. If the file is compressed, this will show compressed data size.", 0) \
    M(FileSync, "Number of times the F_FULLFSYNC/fsync/fdatasync function was called for files.", 0) \
    M(DirectorySync, "Number of times the F_FULLFSYNC/fsync/fdatasync function was called for directories.", 0) \
    M(FileSyncElapsedMicroseconds, "Total time spent waiting for F_FULLFSYNC/fsync/fdatasync syscall for files.", 0) \
    M(DirectorySyncElapsedMicroseconds, "Total time spent waiting for F_FULLFSYNC/fsync/fdatasync syscall for directories.", 0) \
    M(ReadCompressedBytes, "Number of bytes (the number of bytes before decompression) read from compressed sources (files, network).", 0) \
    M(CompressedReadBufferBlocks, "Number of compressed blocks (the blocks of data that are compressed independent of each other) read from compressed sources (files, network).", 0) \
    M(CompressedReadBufferBytes, "Number of uncompressed bytes (the number of bytes after decompression) read from compressed sources (files, network).", 0) \
    M(UncompressedCacheHits, "Number of times a block of data has been found in the uncompressed cache (and decompression was avoided).", 0) \
    M(UncompressedCacheMisses, "Number of times a block of data has not been found in the uncompressed cache (and required decompression).", 0) \
    M(UncompressedCacheWeightLost, "Number of bytes evicted from the uncompressed cache.", 0) \
    M(MMappedFileCacheHits, "Number of times a file has been found in the MMap cache (for the 'mmap' read_method), so we didn't have to mmap it again.", 0) \
    M(MMappedFileCacheMisses, "Number of times a file has not been found in the MMap cache (for the 'mmap' read_method), so we had to mmap it again.", 0) \
    M(OpenedFileCacheHits, "Number of times a file has been found in the opened file cache, so we didn't have to open it again.", 0) \
    M(OpenedFileCacheMisses, "Number of times a file has been found in the opened file cache, so we had to open it again.", 0) \
    M(OpenedFileCacheMicroseconds, "Amount of time spent executing OpenedFileCache methods.", 0) \
    M(AIOWrite, "Number of writes with Linux or FreeBSD AIO interface", 0) \
    M(AIOWriteBytes, "Number of bytes written with Linux or FreeBSD AIO interface", 0) \
    M(AIORead, "Number of reads with Linux or FreeBSD AIO interface", 0) \
    M(AIOReadBytes, "Number of bytes read with Linux or FreeBSD AIO interface", 0) \
    M(IOBufferAllocs, "Number of allocations of IO buffers (for ReadBuffer/WriteBuffer).", 0) \
    M(IOBufferAllocBytes, "Number of bytes allocated for IO buffers (for ReadBuffer/WriteBuffer).", 0) \
    M(ArenaAllocChunks, "Number of chunks allocated for memory Arena (used for GROUP BY and similar operations)", 0) \
    M(ArenaAllocBytes, "Number of bytes allocated for memory Arena (used for GROUP BY and similar operations)", 0) \
    M(FunctionExecute, "Number of SQL ordinary function calls (SQL functions are called on per-block basis, so this number represents the number of blocks).", 0) \
    M(TableFunctionExecute, "Number of table function calls.", 0) \
    M(MarkCacheHits, "Number of times an entry has been found in the mark cache, so we didn't have to load a mark file.", 0) \
    M(MarkCacheMisses, "Number of times an entry has not been found in the mark cache, so we had to load a mark file in memory, which is a costly operation, adding to query latency.", 0) \
    M(QueryCacheHits, "Number of times a query result has been found in the query cache (and query computation was avoided). Only updated for SELECT queries with SETTING use_query_cache = 1.", 0) \
    M(QueryCacheMisses, "Number of times a query result has not been found in the query cache (and required query computation). Only updated for SELECT queries with SETTING use_query_cache = 1.", 0) \
    /* Each page cache chunk access increments exactly one of the following 5 PageCacheChunk* counters. */ \
    /* Something like hit rate: (PageCacheChunkShared + PageCacheChunkDataHits) / [sum of all 5]. */ \
    M(PageCacheChunkMisses, "Number of times a chunk has not been found in the userspace page cache.", 0) \
    M(PageCacheChunkShared, "Number of times a chunk has been found in the userspace page cache, already in use by another thread.", 0) \
    M(PageCacheChunkDataHits, "Number of times a chunk has been found in the userspace page cache, not in use, with all pages intact.", 0) \
    M(PageCacheChunkDataPartialHits, "Number of times a chunk has been found in the userspace page cache, not in use, but some of its pages were evicted by the OS.", 0) \
    M(PageCacheChunkDataMisses, "Number of times a chunk has been found in the userspace page cache, not in use, but all its pages were evicted by the OS.", 0) \
    M(PageCacheBytesUnpinnedRoundedToPages, "Total size of populated pages in chunks that became evictable in PageCache. Rounded up to whole pages.", 0) \
    M(PageCacheBytesUnpinnedRoundedToHugePages, "See PageCacheBytesUnpinnedRoundedToPages, but rounded to huge pages. Use the ratio between the two as a measure of memory waste from using huge pages.", 0) \
    M(CreatedReadBufferOrdinary, "Number of times ordinary read buffer was created for reading data (while choosing among other read methods).", 0) \
    M(CreatedReadBufferDirectIO, "Number of times a read buffer with O_DIRECT was created for reading data (while choosing among other read methods).", 0) \
    M(CreatedReadBufferDirectIOFailed, "Number of times a read buffer with O_DIRECT was attempted to be created for reading data (while choosing among other read methods), but the OS did not allow it (due to lack of filesystem support or other reasons) and we fallen back to the ordinary reading method.", 0) \
    M(CreatedReadBufferMMap, "Number of times a read buffer using 'mmap' was created for reading data (while choosing among other read methods).", 0) \
    M(CreatedReadBufferMMapFailed, "Number of times a read buffer with 'mmap' was attempted to be created for reading data (while choosing among other read methods), but the OS did not allow it (due to lack of filesystem support or other reasons) and we fallen back to the ordinary reading method.", 0) \
    M(DiskReadElapsedMicroseconds, "Total time spent waiting for read syscall. This include reads from page cache.", 0) \
    M(DiskWriteElapsedMicroseconds, "Total time spent waiting for write syscall. This include writes to page cache.", 0) \
    M(NetworkReceiveElapsedMicroseconds, "Total time spent waiting for data to receive or receiving data from network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.", 0) \
    M(NetworkSendElapsedMicroseconds, "Total time spent waiting for data to send to network or sending data to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.", 0) \
    M(NetworkReceiveBytes, "Total number of bytes received from network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.", 0) \
    M(NetworkSendBytes, "Total number of bytes send to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.", 0) \
    \
    M(GlobalThreadPoolExpansions, "Counts the total number of times new threads have been added to the global thread pool. This metric indicates the frequency of expansions in the global thread pool to accommodate increased processing demands.", 0) \
    M(GlobalThreadPoolShrinks, "Counts the total number of times the global thread pool has shrunk by removing threads. This occurs when the number of idle threads exceeds max_thread_pool_free_size, indicating adjustments in the global thread pool size in response to decreased thread utilization.", 0) \
    M(GlobalThreadPoolThreadCreationMicroseconds, "Total time spent waiting for new threads to start.", 0) \
    M(GlobalThreadPoolLockWaitMicroseconds, "Total time threads have spent waiting for locks in the global thread pool.", 0) \
    M(GlobalThreadPoolJobs, "Counts the number of jobs that have been pushed to the global thread pool.", 0) \
    M(GlobalThreadPoolJobWaitTimeMicroseconds, "Measures the elapsed time from when a job is scheduled in the thread pool to when it is picked up for execution by a worker thread. This metric helps identify delays in job processing, indicating the responsiveness of the thread pool to new tasks.", 0) \
    M(LocalThreadPoolExpansions, "Counts the total number of times threads have been borrowed from the global thread pool to expand local thread pools.", 0) \
    M(LocalThreadPoolShrinks, "Counts the total number of times threads have been returned to the global thread pool from local thread pools.", 0) \
    M(LocalThreadPoolThreadCreationMicroseconds, "Total time local thread pools have spent waiting to borrow a thread from the global pool.", 0) \
    M(LocalThreadPoolLockWaitMicroseconds, "Total time threads have spent waiting for locks in the local thread pools.", 0) \
    M(LocalThreadPoolJobs, "Counts the number of jobs that have been pushed to the local thread pools.", 0) \
    M(LocalThreadPoolBusyMicroseconds, "Total time threads have spent executing the actual work.", 0) \
    M(LocalThreadPoolJobWaitTimeMicroseconds, "Measures the elapsed time from when a job is scheduled in the thread pool to when it is picked up for execution by a worker thread. This metric helps identify delays in job processing, indicating the responsiveness of the thread pool to new tasks.", 0) \
    \
    M(DiskS3GetRequestThrottlerCount, "Number of DiskS3 GET and SELECT requests passed through throttler.", 0) \
    M(DiskS3GetRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform DiskS3 GET and SELECT request throttling.", 0) \
    M(DiskS3PutRequestThrottlerCount, "Number of DiskS3 PUT, COPY, POST and LIST requests passed through throttler.", 0) \
    M(DiskS3PutRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform DiskS3 PUT, COPY, POST and LIST request throttling.", 0) \
    M(S3GetRequestThrottlerCount, "Number of S3 GET and SELECT requests passed through throttler.", 0) \
    M(S3GetRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform S3 GET and SELECT request throttling.", 0) \
    M(S3PutRequestThrottlerCount, "Number of S3 PUT, COPY, POST and LIST requests passed through throttler.", 0) \
    M(S3PutRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform S3 PUT, COPY, POST and LIST request throttling.", 0) \
    M(RemoteReadThrottlerBytes, "Bytes passed through 'max_remote_read_network_bandwidth_for_server'/'max_remote_read_network_bandwidth' throttler.", 0) \
    M(RemoteReadThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_remote_read_network_bandwidth_for_server'/'max_remote_read_network_bandwidth' throttling.", 0) \
    M(RemoteWriteThrottlerBytes, "Bytes passed through 'max_remote_write_network_bandwidth_for_server'/'max_remote_write_network_bandwidth' throttler.", 0) \
    M(RemoteWriteThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_remote_write_network_bandwidth_for_server'/'max_remote_write_network_bandwidth' throttling.", 0) \
    M(LocalReadThrottlerBytes, "Bytes passed through 'max_local_read_bandwidth_for_server'/'max_local_read_bandwidth' throttler.", 0) \
    M(LocalReadThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_local_read_bandwidth_for_server'/'max_local_read_bandwidth' throttling.", 0) \
    M(LocalWriteThrottlerBytes, "Bytes passed through 'max_local_write_bandwidth_for_server'/'max_local_write_bandwidth' throttler.", 0) \
    M(LocalWriteThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_local_write_bandwidth_for_server'/'max_local_write_bandwidth' throttling.", 0) \
    M(ThrottlerSleepMicroseconds, "Total time a query was sleeping to conform all throttling settings.", 0) \
    M(PartsWithAppliedMutationsOnFly, "Total number of parts for which there was any mutation applied on fly", 0) \
    M(MutationsAppliedOnFlyInAllParts, "The sum of number of applied mutations on-fly for part among all read parts", 0) \
    \
    M(SchedulerIOReadRequests, "Resource requests passed through scheduler for IO reads.", 0) \
    M(SchedulerIOReadBytes, "Bytes passed through scheduler for IO reads.", 0) \
    M(SchedulerIOReadWaitMicroseconds, "Total time a query was waiting on resource requests for IO reads.", 0) \
    M(SchedulerIOWriteRequests, "Resource requests passed through scheduler for IO writes.", 0) \
    M(SchedulerIOWriteBytes, "Bytes passed through scheduler for IO writes.", 0) \
    M(SchedulerIOWriteWaitMicroseconds, "Total time a query was waiting on resource requests for IO writes.", 0) \
    \
    M(QueryMaskingRulesMatch, "Number of times query masking rules was successfully matched.", 0) \
    \
    M(ReplicatedPartFetches, "Number of times a data part was downloaded from replica of a ReplicatedMergeTree table.", 0) \
    M(ReplicatedPartFailedFetches, "Number of times a data part was failed to download from replica of a ReplicatedMergeTree table.", 0) \
    M(ObsoleteReplicatedParts, "Number of times a data part was covered by another data part that has been fetched from a replica (so, we have marked a covered data part as obsolete and no longer needed).", 0) \
    M(ReplicatedPartMerges, "Number of times data parts of ReplicatedMergeTree tables were successfully merged.", 0) \
    M(ReplicatedPartFetchesOfMerged, "Number of times we prefer to download already merged part from replica of ReplicatedMergeTree table instead of performing a merge ourself (usually we prefer doing a merge ourself to save network traffic). This happens when we have not all source parts to perform a merge or when the data part is old enough.", 0) \
    M(ReplicatedPartMutations, "Number of times data parts of ReplicatedMergeTree tables were successfully mutated.", 0) \
    M(ReplicatedPartChecks, "Number of times we had to perform advanced search for a data part on replicas or to clarify the need of an existing data part.", 0) \
    M(ReplicatedPartChecksFailed, "Number of times the advanced search for a data part on replicas did not give result or when unexpected part has been found and moved away.", 0) \
    M(ReplicatedDataLoss, "Number of times a data part that we wanted doesn't exist on any replica (even on replicas that are offline right now). That data parts are definitely lost. This is normal due to asynchronous replication (if quorum inserts were not enabled), when the replica on which the data part was written was failed and when it became online after fail it doesn't contain that data part.", 0) \
    M(ReplicatedCoveredPartsInZooKeeperOnStart, "For debugging purposes. Number of parts in ZooKeeper that have a covering part, but doesn't exist on disk. Checked on server start.", 0) \
    \
    M(InsertedRows, "Number of rows INSERTed to all tables.", 0) \
    M(InsertedBytes, "Number of bytes (uncompressed; for columns as they stored in memory) INSERTed to all tables.", 0) \
    M(DelayedInserts, "Number of times the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.", 0) \
    M(RejectedInserts, "Number of times the INSERT of a block to a MergeTree table was rejected with 'Too many parts' exception due to high number of active data parts for partition.", 0) \
    M(DelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.", 0) \
    M(DelayedMutations, "Number of times the mutation of a MergeTree table was throttled due to high number of unfinished mutations for table.", 0) \
    M(RejectedMutations, "Number of times the mutation of a MergeTree table was rejected with 'Too many mutations' exception due to high number of unfinished mutations for table.", 0) \
    M(DelayedMutationsMilliseconds, "Total number of milliseconds spent while the mutation of a MergeTree table was throttled due to high number of unfinished mutations for table.", 0) \
    M(DistributedDelayedInserts, "Number of times the INSERT of a block to a Distributed table was throttled due to high number of pending bytes.", 0) \
    M(DistributedRejectedInserts, "Number of times the INSERT of a block to a Distributed table was rejected with 'Too many bytes' exception due to high number of pending bytes.", 0) \
    M(DistributedDelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a Distributed table was throttled due to high number of pending bytes.", 0) \
    M(DuplicatedInsertedBlocks, "Number of times the INSERTed block to a ReplicatedMergeTree table was deduplicated.", 0) \
    \
    M(ZooKeeperInit, "Number of times connection with ZooKeeper has been established.", 0) \
    M(ZooKeeperTransactions, "Number of ZooKeeper operations, which include both read and write operations as well as multi-transactions.", 0) \
    M(ZooKeeperList, "Number of 'list' (getChildren) requests to ZooKeeper.", 0) \
    M(ZooKeeperCreate, "Number of 'create' requests to ZooKeeper.", 0) \
    M(ZooKeeperRemove, "Number of 'remove' requests to ZooKeeper.", 0) \
    M(ZooKeeperExists, "Number of 'exists' requests to ZooKeeper.", 0) \
    M(ZooKeeperGet, "Number of 'get' requests to ZooKeeper.", 0) \
    M(ZooKeeperSet, "Number of 'set' requests to ZooKeeper.", 0) \
    M(ZooKeeperMulti, "Number of 'multi' requests to ZooKeeper (compound transactions).", 0) \
    M(ZooKeeperCheck, "Number of 'check' requests to ZooKeeper. Usually they don't make sense in isolation, only as part of a complex transaction.", 0) \
    M(ZooKeeperSync, "Number of 'sync' requests to ZooKeeper. These requests are rarely needed or usable.", 0) \
    M(ZooKeeperReconfig, "Number of 'reconfig' requests to ZooKeeper.", 0) \
    M(ZooKeeperClose, "Number of times connection with ZooKeeper has been closed voluntary.", 0) \
    M(ZooKeeperWatchResponse, "Number of times watch notification has been received from ZooKeeper.", 0) \
    M(ZooKeeperUserExceptions, "Number of exceptions while working with ZooKeeper related to the data (no node, bad version or similar).", 0) \
    M(ZooKeeperHardwareExceptions, "Number of exceptions while working with ZooKeeper related to network (connection loss or similar).", 0) \
    M(ZooKeeperOtherExceptions, "Number of exceptions while working with ZooKeeper other than ZooKeeperUserExceptions and ZooKeeperHardwareExceptions.", 0) \
    M(ZooKeeperWaitMicroseconds, "Number of microseconds spent waiting for responses from ZooKeeper after creating a request, summed across all the requesting threads.", 0) \
    M(ZooKeeperBytesSent, "Number of bytes send over network while communicating with ZooKeeper.", 0) \
    M(ZooKeeperBytesReceived, "Number of bytes received over network while communicating with ZooKeeper.", 0) \
    \
    M(DistributedConnectionTries, "Total count of distributed connection attempts.", 0) \
    M(DistributedConnectionUsable, "Total count of successful distributed connections to a usable server (with required table, but maybe stale).", 0) \
    M(DistributedConnectionFailTry, "Total count when distributed connection fails with retry.", 0) \
    M(DistributedConnectionMissingTable, "Number of times we rejected a replica from a distributed query, because it did not contain a table needed for the query.", 0) \
    M(DistributedConnectionStaleReplica, "Number of times we rejected a replica from a distributed query, because some table needed for a query had replication lag higher than the configured threshold.", 0) \
    M(DistributedConnectionSkipReadOnlyReplica, "Number of replicas skipped during INSERT into Distributed table due to replicas being read-only", 0) \
    M(DistributedConnectionFailAtAll, "Total count when distributed connection fails after all retries finished.", 0) \
    \
    M(HedgedRequestsChangeReplica, "Total count when timeout for changing replica expired in hedged requests.", 0) \
    M(SuspendSendingQueryToShard, "Total count when sending query to shard was suspended when async_query_sending_for_remote is enabled.", 0) \
    \
    M(CompileFunction, "Number of times a compilation of generated LLVM code (to create fused function for complex expressions) was initiated.", 0) \
    M(CompiledFunctionExecute, "Number of times a compiled function was executed.", 0) \
    M(CompileExpressionsMicroseconds, "Total time spent for compilation of expressions to LLVM code.", 0) \
    M(CompileExpressionsBytes, "Number of bytes used for expressions compilation.", 0) \
    \
    M(ExecuteShellCommand, "Number of shell command executions.", 0) \
    \
    M(ExternalProcessingCompressedBytesTotal, "Number of compressed bytes written by external processing (sorting/aggragating/joining)", 0) \
    M(ExternalProcessingUncompressedBytesTotal, "Amount of data (uncompressed, before compression) written by external processing (sorting/aggragating/joining)", 0) \
    M(ExternalProcessingFilesTotal, "Number of files used by external processing (sorting/aggragating/joining)", 0) \
    M(ExternalSortWritePart, "Number of times a temporary file was written to disk for sorting in external memory.", 0) \
    M(ExternalSortMerge, "Number of times temporary files were merged for sorting in external memory.", 0) \
    M(ExternalSortCompressedBytes, "Number of compressed bytes written for sorting in external memory.", 0) \
    M(ExternalSortUncompressedBytes, "Amount of data (uncompressed, before compression) written for sorting in external memory.", 0) \
    M(ExternalAggregationWritePart, "Number of times a temporary file was written to disk for aggregation in external memory.", 0) \
    M(ExternalAggregationMerge, "Number of times temporary files were merged for aggregation in external memory.", 0) \
    M(ExternalAggregationCompressedBytes, "Number of bytes written to disk for aggregation in external memory.", 0) \
    M(ExternalAggregationUncompressedBytes, "Amount of data (uncompressed, before compression) written to disk for aggregation in external memory.", 0) \
    M(ExternalJoinWritePart, "Number of times a temporary file was written to disk for JOIN in external memory.", 0) \
    M(ExternalJoinMerge, "Number of times temporary files were merged for JOIN in external memory.", 0) \
    M(ExternalJoinCompressedBytes, "Number of compressed bytes written for JOIN in external memory.", 0) \
    M(ExternalJoinUncompressedBytes, "Amount of data (uncompressed, before compression) written for JOIN in external memory.", 0) \
    \
    M(SlowRead, "Number of reads from a file that were slow. This indicate system overload. Thresholds are controlled by read_backoff_* settings.", 0) \
    M(ReadBackoff, "Number of times the number of query processing threads was lowered due to slow reads.", 0) \
    \
    M(ReplicaPartialShutdown, "How many times Replicated table has to deinitialize its state due to session expiration in ZooKeeper. The state is reinitialized every time when ZooKeeper is available again.", 0) \
    \
    M(SelectedParts, "Number of data parts selected to read from a MergeTree table.", 0) \
    M(SelectedPartsTotal, "Number of total data parts before selecting which ones to read from a MergeTree table.", 0) \
    M(SelectedRanges, "Number of (non-adjacent) ranges in all data parts selected to read from a MergeTree table.", 0) \
    M(SelectedMarks, "Number of marks (index granules) selected to read from a MergeTree table.", 0) \
    M(SelectedMarksTotal, "Number of total marks (index granules) before selecting which ones to read from a MergeTree table.", 0) \
    M(SelectedRows, "Number of rows SELECTed from all tables.", 0) \
    M(SelectedBytes, "Number of bytes (uncompressed; for columns as they stored in memory) SELECTed from all tables.", 0) \
    M(RowsReadByMainReader, "Number of rows read from MergeTree tables by the main reader (after PREWHERE step).", 0) \
    M(RowsReadByPrewhereReaders, "Number of rows read from MergeTree tables (in total) by prewhere readers.", 0) \
    \
    M(WaitMarksLoadMicroseconds, "Time spent loading marks", 0) \
    M(BackgroundLoadingMarksTasks, "Number of background tasks for loading marks", 0) \
    M(LoadedMarksCount, "Number of marks loaded (total across columns).", 0) \
    M(LoadedMarksMemoryBytes, "Size of in-memory representations of loaded marks.", 0) \
    \
    M(Merge, "Number of launched background merges.", 0) \
    M(MergedRows, "Rows read for background merges. This is the number of rows before merge.", 0) \
    M(MergedColumns, "Number of columns merged during the horizontal stage of merges.", 0) \
    M(GatheredColumns, "Number of columns gathered during the vertical stage of merges.", 0) \
    M(MergedUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) that was read for background merges. This is the number before merge.", 0) \
    M(MergeTotalMilliseconds, "Total time spent for background merges", 0) \
    M(MergeExecuteMilliseconds, "Total busy time spent for execution of background merges", 0) \
    M(MergeHorizontalStageTotalMilliseconds, "Total time spent for horizontal stage of background merges", 0) \
    M(MergeHorizontalStageExecuteMilliseconds, "Total busy time spent for execution of horizontal stage of background merges", 0) \
    M(MergeVerticalStageTotalMilliseconds, "Total time spent for vertical stage of background merges", 0) \
    M(MergeVerticalStageExecuteMilliseconds, "Total busy time spent for execution of vertical stage of background merges", 0) \
    M(MergeProjectionStageTotalMilliseconds, "Total time spent for projection stage of background merges", 0) \
    M(MergeProjectionStageExecuteMilliseconds, "Total busy time spent for execution of projection stage of background merges", 0) \
    \
    M(MergingSortedMilliseconds, "Total time spent while merging sorted columns", 0) \
    M(AggregatingSortedMilliseconds, "Total time spent while aggregating sorted columns", 0) \
    M(CollapsingSortedMilliseconds, "Total time spent while collapsing sorted columns", 0) \
    M(ReplacingSortedMilliseconds, "Total time spent while replacing sorted columns", 0) \
    M(SummingSortedMilliseconds, "Total time spent while summing sorted columns", 0) \
    M(VersionedCollapsingSortedMilliseconds, "Total time spent while version collapsing sorted columns", 0) \
    M(GatheringColumnMilliseconds, "Total time spent while gathering columns for vertical merge", 0) \
    \
    M(MutationTotalParts, "Number of total parts for which mutations tried to be applied", 0) \
    M(MutationUntouchedParts, "Number of total parts for which mutations tried to be applied but which was completely skipped according to predicate", 0) \
    M(MutatedRows, "Rows read for mutations. This is the number of rows before mutation", 0) \
    M(MutatedUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) that was read for mutations. This is the number before mutation.", 0) \
    M(MutationTotalMilliseconds, "Total time spent for mutations.", 0) \
    M(MutationExecuteMilliseconds, "Total busy time spent for execution of mutations.", 0) \
    M(MutationAllPartColumns, "Number of times when task to mutate all columns in part was created", 0) \
    M(MutationSomePartColumns, "Number of times when task to mutate some columns in part was created", 0) \
    M(MutateTaskProjectionsCalculationMicroseconds, "Time spent calculating projections in mutations.", 0) \
    \
    M(MergeTreeDataWriterRows, "Number of rows INSERTed to MergeTree tables.", 0) \
    M(MergeTreeDataWriterUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables.", 0) \
    M(MergeTreeDataWriterCompressedBytes, "Bytes written to filesystem for data INSERTed to MergeTree tables.", 0) \
    M(MergeTreeDataWriterBlocks, "Number of blocks INSERTed to MergeTree tables. Each block forms a data part of level zero.", 0) \
    M(MergeTreeDataWriterBlocksAlreadySorted, "Number of blocks INSERTed to MergeTree tables that appeared to be already sorted.", 0) \
    \
    M(MergeTreeDataWriterSkipIndicesCalculationMicroseconds, "Time spent calculating skip indices", 0) \
    M(MergeTreeDataWriterStatisticsCalculationMicroseconds, "Time spent calculating statistics", 0) \
    M(MergeTreeDataWriterSortingBlocksMicroseconds, "Time spent sorting blocks", 0) \
    M(MergeTreeDataWriterMergingBlocksMicroseconds, "Time spent merging input blocks (for special MergeTree engines)", 0) \
    M(MergeTreeDataWriterProjectionsCalculationMicroseconds, "Time spent calculating projections", 0) \
    M(MergeTreeDataProjectionWriterSortingBlocksMicroseconds, "Time spent sorting blocks (for projection it might be a key different from table's sorting key)", 0) \
    M(MergeTreeDataProjectionWriterMergingBlocksMicroseconds, "Time spent merging blocks", 0) \
    \
    M(InsertedWideParts, "Number of parts inserted in Wide format.", 0) \
    M(InsertedCompactParts, "Number of parts inserted in Compact format.", 0) \
    M(MergedIntoWideParts, "Number of parts merged into Wide format.", 0) \
    M(MergedIntoCompactParts, "Number of parts merged into Compact format.", 0) \
    \
    M(MergeTreeDataProjectionWriterRows, "Number of rows INSERTed to MergeTree tables projection.", 0) \
    M(MergeTreeDataProjectionWriterUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables projection.", 0) \
    M(MergeTreeDataProjectionWriterCompressedBytes, "Bytes written to filesystem for data INSERTed to MergeTree tables projection.", 0) \
    M(MergeTreeDataProjectionWriterBlocks, "Number of blocks INSERTed to MergeTree tables projection. Each block forms a data part of level zero.", 0) \
    M(MergeTreeDataProjectionWriterBlocksAlreadySorted, "Number of blocks INSERTed to MergeTree tables projection that appeared to be already sorted.", 0) \
    \
    M(CannotRemoveEphemeralNode, "Number of times an error happened while trying to remove ephemeral node. This is not an issue, because our implementation of ZooKeeper library guarantee that the session will expire and the node will be removed.", 0) \
    \
    M(RegexpWithMultipleNeedlesCreated, "Regular expressions with multiple needles (VectorScan library) compiled.", 0) \
    M(RegexpWithMultipleNeedlesGlobalCacheHit, "Number of times we fetched compiled regular expression with multiple needles (VectorScan library) from the global cache.", 0) \
    M(RegexpWithMultipleNeedlesGlobalCacheMiss, "Number of times we failed to fetch compiled regular expression with multiple needles (VectorScan library) from the global cache.", 0) \
    M(RegexpLocalCacheHit, "Number of times we fetched compiled regular expression from a local cache.", 0) \
    M(RegexpLocalCacheMiss, "Number of times we failed to fetch compiled regular expression from a local cache.", 0) \
    \
    M(ContextLock, "Number of times the lock of Context was acquired or tried to acquire. This is global lock.", 0) \
    M(ContextLockWaitMicroseconds, "Context lock wait time in microseconds", 0) \
    \
    M(StorageBufferFlush, "Number of times a buffer in a 'Buffer' table was flushed.", 0) \
    M(StorageBufferErrorOnFlush, "Number of times a buffer in the 'Buffer' table has not been able to flush due to error writing in the destination table.", 0) \
    M(StorageBufferPassedAllMinThresholds, "Number of times a criteria on min thresholds has been reached to flush a buffer in a 'Buffer' table.", 0) \
    M(StorageBufferPassedTimeMaxThreshold, "Number of times a criteria on max time threshold has been reached to flush a buffer in a 'Buffer' table.", 0) \
    M(StorageBufferPassedRowsMaxThreshold, "Number of times a criteria on max rows threshold has been reached to flush a buffer in a 'Buffer' table.", 0) \
    M(StorageBufferPassedBytesMaxThreshold, "Number of times a criteria on max bytes threshold has been reached to flush a buffer in a 'Buffer' table.", 0) \
    M(StorageBufferPassedTimeFlushThreshold, "Number of times background-only flush threshold on time has been reached to flush a buffer in a 'Buffer' table. This is expert-only metric. If you read this and you are not an expert, stop reading.", 0) \
    M(StorageBufferPassedRowsFlushThreshold, "Number of times background-only flush threshold on rows has been reached to flush a buffer in a 'Buffer' table. This is expert-only metric. If you read this and you are not an expert, stop reading.", 0) \
    M(StorageBufferPassedBytesFlushThreshold, "Number of times background-only flush threshold on bytes has been reached to flush a buffer in a 'Buffer' table. This is expert-only metric. If you read this and you are not an expert, stop reading.", 0) \
    M(StorageBufferLayerLockReadersWaitMilliseconds, "Time for waiting for Buffer layer during reading.", 0) \
    M(StorageBufferLayerLockWritersWaitMilliseconds, "Time for waiting free Buffer layer to write to (can be used to tune Buffer layers).", 0) \
    \
    M(DictCacheKeysRequested, "Number of keys requested from the data source for the dictionaries of 'cache' types.", 0) \
    M(DictCacheKeysRequestedMiss, "Number of keys requested from the data source for dictionaries of 'cache' types but not found in the data source.", 0) \
    M(DictCacheKeysRequestedFound, "Number of keys requested from the data source for dictionaries of 'cache' types and found in the data source.", 0) \
    M(DictCacheKeysExpired, "Number of keys looked up in the dictionaries of 'cache' types and found in the cache but they were obsolete.", 0) \
    M(DictCacheKeysNotFound, "Number of keys looked up in the dictionaries of 'cache' types and not found.", 0) \
    M(DictCacheKeysHit, "Number of keys looked up in the dictionaries of 'cache' types and found in the cache.", 0) \
    M(DictCacheRequestTimeNs, "Number of nanoseconds spend in querying the external data sources for the dictionaries of 'cache' types.", 0) \
    M(DictCacheRequests, "Number of bulk requests to the external data sources for the dictionaries of 'cache' types.", 0) \
    M(DictCacheLockWriteNs, "Number of nanoseconds spend in waiting for write lock to update the data for the dictionaries of 'cache' types.", 0) \
    M(DictCacheLockReadNs, "Number of nanoseconds spend in waiting for read lock to lookup the data for the dictionaries of 'cache' types.", 0) \
    \
    M(DistributedSyncInsertionTimeoutExceeded, "A timeout has exceeded while waiting for shards during synchronous insertion into a Distributed table (with 'distributed_foreground_insert' = 1)", 0) \
    M(DistributedAsyncInsertionFailures, "Number of failures for asynchronous insertion into a Distributed table (with 'distributed_foreground_insert' = 0)", 0) \
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
)", 0) \
    M(DataAfterMutationDiffersFromReplica, "Number of times data after mutation is not byte-identical to the data on other replicas. In addition to the reasons described in 'DataAfterMergeDiffersFromReplica', it is also possible due to non-deterministic mutation.", 0) \
    M(PolygonsAddedToPool, "A polygon has been added to the cache (pool) for the 'pointInPolygon' function.", 0) \
    M(PolygonsInPoolAllocatedBytes, "The number of bytes for polygons added to the cache (pool) for the 'pointInPolygon' function.", 0) \
    \
    M(USearchAddCount, "Number of vectors added to usearch indexes.", 0) \
    M(USearchAddVisitedMembers, "Number of nodes visited when adding vectors to usearch indexes.", 0) \
    M(USearchAddComputedDistances, "Number of times distance was computed when adding vectors to usearch indexes.", 0) \
    M(USearchSearchCount, "Number of search operations performed in usearch indexes.", 0) \
    M(USearchSearchVisitedMembers, "Number of nodes visited when searching in usearch indexes.", 0) \
    M(USearchSearchComputedDistances, "Number of times distance was computed when searching usearch indexes.", 0) \
    \
    M(RWLockAcquiredReadLocks, "Number of times a read lock was acquired (in a heavy RWLock).", 0) \
    M(RWLockAcquiredWriteLocks, "Number of times a write lock was acquired (in a heavy RWLock).", 0) \
    M(RWLockReadersWaitMilliseconds, "Total time spent waiting for a read lock to be acquired (in a heavy RWLock).", 0) \
    M(RWLockWritersWaitMilliseconds, "Total time spent waiting for a write lock to be acquired (in a heavy RWLock).", 0) \
    M(DNSError, "Total count of errors in DNS resolution", 0) \
    M(PartsLockHoldMicroseconds, "Total time spent holding data parts lock in MergeTree tables", 0) \
    M(PartsLockWaitMicroseconds, "Total time spent waiting for data parts lock in MergeTree tables", 0) \
    \
    M(RealTimeMicroseconds, "Total (wall clock) time spent in processing (queries and other tasks) threads (note that this is a sum).", 0) \
    M(UserTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in user mode. This includes time CPU pipeline was stalled due to main memory access, cache misses, branch mispredictions, hyper-threading, etc.", 0) \
    M(SystemTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in OS kernel mode. This is time spent in syscalls, excluding waiting time during blocking syscalls.", 0) \
    M(MemoryOvercommitWaitTimeMicroseconds, "Total time spent in waiting for memory to be freed in OvercommitTracker.", 0) \
    M(MemoryAllocatorPurge, "Total number of times memory allocator purge was requested", 0) \
    M(MemoryAllocatorPurgeTimeMicroseconds, "Total number of times memory allocator purge was requested", 0) \
    M(SoftPageFaults, "The number of soft page faults in query execution threads. Soft page fault usually means a miss in the memory allocator cache, which requires a new memory mapping from the OS and subsequent allocation of a page of physical memory.", 0) \
    M(HardPageFaults, "The number of hard page faults in query execution threads. High values indicate either that you forgot to turn off swap on your server, or eviction of memory pages of the ClickHouse binary during very high memory pressure, or successful usage of the 'mmap' read method for the tables data.", 0) \
    \
    M(OSIOWaitMicroseconds, "Total time a thread spent waiting for a result of IO operation, from the OS point of view. This is real IO that doesn't include page cache.", 0) \
    M(OSCPUWaitMicroseconds, "Total time a thread was ready for execution but waiting to be scheduled by OS, from the OS point of view.", 10) \
    M(OSCPUVirtualTimeMicroseconds, "CPU time spent seen by OS. Does not include involuntary waits due to virtualization.", 10) \
    M(OSReadBytes, "Number of bytes read from disks or block devices. Doesn't include bytes read from page cache. May include excessive data due to block size, readahead, etc.", 0) \
    M(OSWriteBytes, "Number of bytes written to disks or block devices. Doesn't include bytes that are in page cache dirty pages. May not include data that was written by OS asynchronously.", 0) \
    M(OSReadChars, "Number of bytes read from filesystem, including page cache.", 0) \
    M(OSWriteChars, "Number of bytes written to filesystem, including page cache.", 0) \
    \
    M(ParallelReplicasHandleRequestMicroseconds, "Time spent processing requests for marks from replicas", 0) \
    M(ParallelReplicasHandleAnnouncementMicroseconds, "Time spent processing replicas announcements", 0) \
    M(ParallelReplicasAnnouncementMicroseconds, "Time spent to send an announcement", 0) \
    M(ParallelReplicasReadRequestMicroseconds, "Time spent for read requests", 0) \
    \
    M(ParallelReplicasReadAssignedMarks, "Sum across all replicas of how many of scheduled marks were assigned by consistent hash", 0) \
    M(ParallelReplicasReadUnassignedMarks, "Sum across all replicas of how many unassigned marks were scheduled", 0) \
    M(ParallelReplicasReadAssignedForStealingMarks, "Sum across all replicas of how many of scheduled marks were assigned for stealing by consistent hash", 0) \
    M(ParallelReplicasReadMarks, "How many marks were read by the given replica", 0) \
    \
    M(ParallelReplicasStealingByHashMicroseconds, "Time spent collecting segments meant for stealing by hash", 0) \
    M(ParallelReplicasProcessingPartsMicroseconds, "Time spent processing data parts", 0) \
    M(ParallelReplicasStealingLeftoversMicroseconds, "Time spent collecting orphaned segments", 0) \
    M(ParallelReplicasCollectingOwnedSegmentsMicroseconds, "Time spent collecting segments meant by hash", 0) \
    M(ParallelReplicasNumRequests, "Number of requests to the initiator.", 0) \
    M(ParallelReplicasDeniedRequests, "Number of completely denied requests to the initiator", 0) \
    M(CacheWarmerBytesDownloaded, "Amount of data fetched into filesystem cache by dedicated background threads.", 0) \
    M(CacheWarmerDataPartsDownloaded, "Number of data parts that were fully fetched by CacheWarmer.", 0) \
    M(IgnoredColdParts, "See setting ignore_cold_parts_seconds. Number of times read queries ignored very new parts that weren't pulled into cache by CacheWarmer yet.", 0) \
    M(PreferredWarmedUnmergedParts, "See setting prefer_warmed_unmerged_parts_seconds. Number of times read queries used outdated pre-merge parts that are in cache instead of merged part that wasn't pulled into cache by CacheWarmer yet.", 0) \
    \
    M(PerfCPUCycles, "Total cycles. Be wary of what happens during CPU frequency scaling.", 0)  \
    M(PerfInstructions, "Retired instructions. Be careful, these can be affected by various issues, most notably hardware interrupt counts.", 0) \
    M(PerfCacheReferences, "Cache accesses. Usually, this indicates Last Level Cache accesses, but this may vary depending on your CPU. This may include prefetches and coherency messages; again this depends on the design of your CPU.", 0) \
    M(PerfCacheMisses, "Cache misses. Usually this indicates Last Level Cache misses; this is intended to be used in conjunction with the PERFCOUNTHWCACHEREFERENCES event to calculate cache miss rates.", 0) \
    M(PerfBranchInstructions, "Retired branch instructions. Prior to Linux 2.6.35, this used the wrong event on AMD processors.", 0) \
    M(PerfBranchMisses, "Mispredicted branch instructions.", 0) \
    M(PerfBusCycles, "Bus cycles, which can be different from total cycles.", 0) \
    M(PerfStalledCyclesFrontend, "Stalled cycles during issue.", 0) \
    M(PerfStalledCyclesBackend, "Stalled cycles during retirement.", 0) \
    M(PerfRefCPUCycles, "Total cycles; not affected by CPU frequency scaling.", 0) \
    \
    M(PerfCPUClock, "The CPU clock, a high-resolution per-CPU timer", 0) \
    M(PerfTaskClock, "A clock count specific to the task that is running", 0) \
    M(PerfContextSwitches, "Number of context switches", 0) \
    M(PerfCPUMigrations, "Number of times the process has migrated to a new CPU", 0) \
    M(PerfAlignmentFaults, "Number of alignment faults. These happen when unaligned memory accesses happen; the kernel can handle these but it reduces performance. This happens only on some architectures (never on x86).", 0) \
    M(PerfEmulationFaults, "Number of emulation faults. The kernel sometimes traps on unimplemented instructions and emulates them for user space. This can negatively impact performance.", 0) \
    M(PerfMinEnabledTime, "For all events, minimum time that an event was enabled. Used to track event multiplexing influence", 0) \
    M(PerfMinEnabledRunningTime, "Running time for event with minimum enabled time. Used to track the amount of event multiplexing", 0) \
    M(PerfDataTLBReferences, "Data TLB references", 0) \
    M(PerfDataTLBMisses, "Data TLB misses", 0) \
    M(PerfInstructionTLBReferences, "Instruction TLB references", 0) \
    M(PerfInstructionTLBMisses, "Instruction TLB misses", 0) \
    M(PerfLocalMemoryReferences, "Local NUMA node memory reads", 0) \
    M(PerfLocalMemoryMisses, "Local NUMA node memory read misses", 0) \
    \
    M(CannotWriteToWriteBufferDiscard, "Number of stack traces dropped by query profiler or signal handler because pipe is full or cannot write to pipe.", 0) \
    M(QueryProfilerSignalOverruns, "Number of times we drop processing of a query profiler signal due to overrun plus the number of signals that OS has not delivered due to overrun.", 0) \
    M(QueryProfilerConcurrencyOverruns, "Number of times we drop processing of a query profiler signal due to too many concurrent query profilers in other threads, which may indicate overload.", 0) \
    M(QueryProfilerRuns, "Number of times QueryProfiler had been run.", 0) \
    M(QueryProfilerErrors, "Invalid memory accesses during asynchronous stack unwinding.", 0) \
    \
    M(CreatedLogEntryForMerge, "Successfully created log entry to merge parts in ReplicatedMergeTree.", 0) \
    M(NotCreatedLogEntryForMerge, "Log entry to merge parts in ReplicatedMergeTree is not created due to concurrent log update by another replica.", 0) \
    M(CreatedLogEntryForMutation, "Successfully created log entry to mutate parts in ReplicatedMergeTree.", 0) \
    M(NotCreatedLogEntryForMutation, "Log entry to mutate parts in ReplicatedMergeTree is not created due to concurrent log update by another replica.", 0) \
    \
    M(S3ReadMicroseconds, "Time of GET and HEAD requests to S3 storage.", 0) \
    M(S3ReadRequestsCount, "Number of GET and HEAD requests to S3 storage.", 0) \
    M(S3ReadRequestsErrors, "Number of non-throttling errors in GET and HEAD requests to S3 storage.", 0) \
    M(S3ReadRequestsThrottling, "Number of 429 and 503 errors in GET and HEAD requests to S3 storage.", 0) \
    M(S3ReadRequestsRedirects, "Number of redirects in GET and HEAD requests to S3 storage.", 0) \
    \
    M(S3WriteMicroseconds, "Time of POST, DELETE, PUT and PATCH requests to S3 storage.", 0) \
    M(S3WriteRequestsCount, "Number of POST, DELETE, PUT and PATCH requests to S3 storage.", 0) \
    M(S3WriteRequestsErrors, "Number of non-throttling errors in POST, DELETE, PUT and PATCH requests to S3 storage.", 0) \
    M(S3WriteRequestsThrottling, "Number of 429 and 503 errors in POST, DELETE, PUT and PATCH requests to S3 storage.", 0) \
    M(S3WriteRequestsRedirects, "Number of redirects in POST, DELETE, PUT and PATCH requests to S3 storage.", 0) \
    \
    M(DiskS3ReadMicroseconds, "Time of GET and HEAD requests to DiskS3 storage.", 0) \
    M(DiskS3ReadRequestsCount, "Number of GET and HEAD requests to DiskS3 storage.", 0) \
    M(DiskS3ReadRequestsErrors, "Number of non-throttling errors in GET and HEAD requests to DiskS3 storage.", 0) \
    M(DiskS3ReadRequestsThrottling, "Number of 429 and 503 errors in GET and HEAD requests to DiskS3 storage.", 0) \
    M(DiskS3ReadRequestsRedirects, "Number of redirects in GET and HEAD requests to DiskS3 storage.", 0) \
    \
    M(DiskS3WriteMicroseconds, "Time of POST, DELETE, PUT and PATCH requests to DiskS3 storage.", 0) \
    M(DiskS3WriteRequestsCount, "Number of POST, DELETE, PUT and PATCH requests to DiskS3 storage.", 0) \
    M(DiskS3WriteRequestsErrors, "Number of non-throttling errors in POST, DELETE, PUT and PATCH requests to DiskS3 storage.", 0) \
    M(DiskS3WriteRequestsThrottling, "Number of 429 and 503 errors in POST, DELETE, PUT and PATCH requests to DiskS3 storage.", 0) \
    M(DiskS3WriteRequestsRedirects, "Number of redirects in POST, DELETE, PUT and PATCH requests to DiskS3 storage.", 0) \
    \
    M(S3DeleteObjects, "Number of S3 API DeleteObject(s) calls.", 0) \
    M(S3CopyObject, "Number of S3 API CopyObject calls.", 0) \
    M(S3ListObjects, "Number of S3 API ListObjects calls.", 0) \
    M(S3HeadObject,  "Number of S3 API HeadObject calls.", 0) \
    M(S3GetObjectAttributes, "Number of S3 API GetObjectAttributes calls.", 0) \
    M(S3CreateMultipartUpload, "Number of S3 API CreateMultipartUpload calls.", 0) \
    M(S3UploadPartCopy, "Number of S3 API UploadPartCopy calls.", 0) \
    M(S3UploadPart, "Number of S3 API UploadPart calls.", 0) \
    M(S3AbortMultipartUpload, "Number of S3 API AbortMultipartUpload calls.", 0) \
    M(S3CompleteMultipartUpload, "Number of S3 API CompleteMultipartUpload calls.", 0) \
    M(S3PutObject, "Number of S3 API PutObject calls.", 0) \
    M(S3GetObject, "Number of S3 API GetObject calls.", 0) \
    \
    M(DiskS3DeleteObjects, "Number of DiskS3 API DeleteObject(s) calls.", 0) \
    M(DiskS3CopyObject, "Number of DiskS3 API CopyObject calls.", 0) \
    M(DiskS3ListObjects, "Number of DiskS3 API ListObjects calls.", 0) \
    M(DiskS3HeadObject,  "Number of DiskS3 API HeadObject calls.", 0) \
    M(DiskS3GetObjectAttributes, "Number of DiskS3 API GetObjectAttributes calls.", 0) \
    M(DiskS3CreateMultipartUpload, "Number of DiskS3 API CreateMultipartUpload calls.", 0) \
    M(DiskS3UploadPartCopy, "Number of DiskS3 API UploadPartCopy calls.", 0) \
    M(DiskS3UploadPart, "Number of DiskS3 API UploadPart calls.", 0) \
    M(DiskS3AbortMultipartUpload, "Number of DiskS3 API AbortMultipartUpload calls.", 0) \
    M(DiskS3CompleteMultipartUpload, "Number of DiskS3 API CompleteMultipartUpload calls.", 0) \
    M(DiskS3PutObject, "Number of DiskS3 API PutObject calls.", 0) \
    M(DiskS3GetObject, "Number of DiskS3 API GetObject calls.", 0) \
    \
    M(DiskPlainRewritableAzureDirectoryCreated, "Number of directories created by the 'plain_rewritable' metadata storage for AzureObjectStorage.", 0) \
    M(DiskPlainRewritableAzureDirectoryRemoved, "Number of directories removed by the 'plain_rewritable' metadata storage for AzureObjectStorage.", 0) \
    M(DiskPlainRewritableLocalDirectoryCreated, "Number of directories created by the 'plain_rewritable' metadata storage for LocalObjectStorage.", 0) \
    M(DiskPlainRewritableLocalDirectoryRemoved, "Number of directories removed by the 'plain_rewritable' metadata storage for LocalObjectStorage.", 0) \
    M(DiskPlainRewritableS3DirectoryCreated, "Number of directories created by the 'plain_rewritable' metadata storage for S3ObjectStorage.", 0) \
    M(DiskPlainRewritableS3DirectoryRemoved, "Number of directories removed by the 'plain_rewritable' metadata storage for S3ObjectStorage.", 0) \
    \
    M(S3Clients, "Number of created S3 clients.", 0) \
    M(TinyS3Clients, "Number of S3 clients copies which reuse an existing auth provider from another client.", 0) \
    \
    M(EngineFileLikeReadFiles, "Number of files read in table engines working with files (like File/S3/URL/HDFS).", 0) \
    \
    M(ReadBufferFromS3Microseconds, "Time spent on reading from S3.", 0) \
    M(ReadBufferFromS3InitMicroseconds, "Time spent initializing connection to S3.", 0) \
    M(ReadBufferFromS3Bytes, "Bytes read from S3.", 0) \
    M(ReadBufferFromS3RequestsErrors, "Number of exceptions while reading from S3.", 0) \
    \
    M(WriteBufferFromS3Microseconds, "Time spent on writing to S3.", 0) \
    M(WriteBufferFromS3Bytes, "Bytes written to S3.", 0) \
    M(WriteBufferFromS3RequestsErrors, "Number of exceptions while writing to S3.", 0) \
    M(WriteBufferFromS3WaitInflightLimitMicroseconds, "Time spent on waiting while some of the current requests are done when its number reached the limit defined by s3_max_inflight_parts_for_one_file.", 0) \
    M(QueryMemoryLimitExceeded, "Number of times when memory limit exceeded for query.", 0) \
    \
    M(AzureGetObject, "Number of Azure API GetObject calls.", 0) \
    M(AzureUpload, "Number of Azure blob storage API Upload calls", 0) \
    M(AzureStageBlock, "Number of Azure blob storage API StageBlock calls", 0) \
    M(AzureCommitBlockList, "Number of Azure blob storage API CommitBlockList calls", 0) \
    M(AzureCopyObject, "Number of Azure blob storage API CopyObject calls", 0) \
    M(AzureDeleteObjects, "Number of Azure blob storage API DeleteObject(s) calls.", 0) \
    M(AzureListObjects, "Number of Azure blob storage API ListObjects calls.", 0) \
    M(AzureGetProperties, "Number of Azure blob storage API GetProperties calls.", 0) \
    M(AzureCreateContainer, "Number of Azure blob storage API CreateContainer calls.", 0) \
    \
    M(DiskAzureGetObject, "Number of Disk Azure API GetObject calls.", 0) \
    M(DiskAzureUpload, "Number of Disk Azure blob storage API Upload calls", 0) \
    M(DiskAzureStageBlock, "Number of Disk Azure blob storage API StageBlock calls", 0) \
    M(DiskAzureCommitBlockList, "Number of Disk Azure blob storage API CommitBlockList calls", 0) \
    M(DiskAzureCopyObject, "Number of Disk Azure blob storage API CopyObject calls", 0) \
    M(DiskAzureListObjects, "Number of Disk Azure blob storage API ListObjects calls.", 0) \
    M(DiskAzureDeleteObjects, "Number of Disk Azure blob storage API DeleteObject(s) calls.", 0) \
    M(DiskAzureGetProperties, "Number of Disk Azure blob storage API GetProperties calls.", 0) \
    M(DiskAzureCreateContainer, "Number of Disk Azure blob storage API CreateContainer calls.", 0) \
    \
    M(ReadBufferFromAzureMicroseconds, "Time spent on reading from Azure.", 0) \
    M(ReadBufferFromAzureInitMicroseconds, "Time spent initializing connection to Azure.", 0) \
    M(ReadBufferFromAzureBytes, "Bytes read from Azure.", 0) \
    M(ReadBufferFromAzureRequestsErrors, "Number of exceptions while reading from Azure", 0) \
    \
    M(CachedReadBufferReadFromCacheHits, "Number of times the read from filesystem cache hit the cache.", 0) \
    M(CachedReadBufferReadFromCacheMisses, "Number of times the read from filesystem cache miss the cache.", 0) \
    M(CachedReadBufferReadFromSourceMicroseconds, "Time reading from filesystem cache source (from remote filesystem, etc)", 0) \
    M(CachedReadBufferReadFromCacheMicroseconds, "Time reading from filesystem cache", 0) \
    M(CachedReadBufferReadFromSourceBytes, "Bytes read from filesystem cache source (from remote fs, etc)", 0) \
    M(CachedReadBufferReadFromCacheBytes, "Bytes read from filesystem cache", 0) \
    M(CachedReadBufferPredownloadedBytes, "Bytes read from filesystem cache source. Cache segments are read from left to right as a whole, it might be that we need to predownload some part of the segment irrelevant for the current task just to get to the needed data", 0) \
    M(CachedReadBufferCacheWriteBytes, "Bytes written from source (remote fs, etc) to filesystem cache", 0) \
    M(CachedReadBufferCacheWriteMicroseconds, "Time spent writing data into filesystem cache", 0) \
    M(CachedReadBufferCreateBufferMicroseconds, "Prepare buffer time", 0) \
    M(CachedWriteBufferCacheWriteBytes, "Bytes written from source (remote fs, etc) to filesystem cache", 0) \
    M(CachedWriteBufferCacheWriteMicroseconds, "Time spent writing data into filesystem cache", 0) \
    \
    M(FilesystemCacheLoadMetadataMicroseconds, "Time spent loading filesystem cache metadata", 0) \
    M(FilesystemCacheEvictedBytes, "Number of bytes evicted from filesystem cache", 0) \
    M(FilesystemCacheEvictedFileSegments, "Number of file segments evicted from filesystem cache", 0) \
    M(FilesystemCacheEvictionSkippedFileSegments, "Number of file segments skipped for eviction because of being in unreleasable state", 0) \
    M(FilesystemCacheEvictionSkippedEvictingFileSegments, "Number of file segments skipped for eviction because of being in evicting state", 0) \
    M(FilesystemCacheEvictionTries, "Number of filesystem cache eviction attempts", 0) \
    M(FilesystemCacheLockKeyMicroseconds, "Lock cache key time", 0) \
    M(FilesystemCacheLockMetadataMicroseconds, "Lock filesystem cache metadata time", 0) \
    M(FilesystemCacheLockCacheMicroseconds, "Lock filesystem cache time", 0) \
    M(FilesystemCacheReserveMicroseconds, "Filesystem cache space reservation time", 0) \
    M(FilesystemCacheEvictMicroseconds, "Filesystem cache eviction time", 0) \
    M(FilesystemCacheGetOrSetMicroseconds, "Filesystem cache getOrSet() time", 0) \
    M(FilesystemCacheGetMicroseconds, "Filesystem cache get() time", 0) \
    M(FileSegmentWaitMicroseconds, "Wait on DOWNLOADING state", 0) \
    M(FileSegmentCompleteMicroseconds, "Duration of FileSegment::complete() in filesystem cache", 0) \
    M(FileSegmentLockMicroseconds, "Lock file segment time", 0) \
    M(FileSegmentWriteMicroseconds, "File segment write() time", 0) \
    M(FileSegmentUseMicroseconds, "File segment use() time", 0) \
    M(FileSegmentRemoveMicroseconds, "File segment remove() time", 0) \
    M(FileSegmentHolderCompleteMicroseconds, "File segments holder complete() time", 0) \
    M(FileSegmentFailToIncreasePriority, "Number of times the priority was not increased due to a high contention on the cache lock", 0) \
    M(FilesystemCacheFailToReserveSpaceBecauseOfLockContention, "Number of times space reservation was skipped due to a high contention on the cache lock", 0) \
    M(FilesystemCacheFailToReserveSpaceBecauseOfCacheResize, "Number of times space reservation was skipped due to the cache is being resized", 0) \
    M(FilesystemCacheHoldFileSegments, "Filesystem cache file segments count, which were hold", 0) \
    M(FilesystemCacheUnusedHoldFileSegments, "Filesystem cache file segments count, which were hold, but not used (because of seek or LIMIT n, etc)", 0) \
    M(FilesystemCacheFreeSpaceKeepingThreadRun, "Number of times background thread executed free space keeping job", 0) \
    M(FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds, "Time for which background thread executed free space keeping job", 0) \
    \
    M(RemoteFSSeeks, "Total number of seeks for async buffer", 0) \
    M(RemoteFSPrefetches, "Number of prefetches made with asynchronous reading from remote filesystem", 0) \
    M(RemoteFSCancelledPrefetches, "Number of cancelled prefecthes (because of seek)", 0) \
    M(RemoteFSUnusedPrefetches, "Number of prefetches pending at buffer destruction", 0) \
    M(RemoteFSPrefetchedReads, "Number of reads from prefecthed buffer", 0) \
    M(RemoteFSPrefetchedBytes, "Number of bytes from prefecthed buffer", 0) \
    M(RemoteFSUnprefetchedReads, "Number of reads from unprefetched buffer", 0) \
    M(RemoteFSUnprefetchedBytes, "Number of bytes from unprefetched buffer", 0) \
    M(RemoteFSLazySeeks, "Number of lazy seeks", 0) \
    M(RemoteFSSeeksWithReset, "Number of seeks which lead to a new connection", 0) \
    M(RemoteFSBuffers, "Number of buffers created for asynchronous reading from remote filesystem", 0) \
    M(MergeTreePrefetchedReadPoolInit, "Time spent preparing tasks in MergeTreePrefetchedReadPool", 0) \
    M(WaitPrefetchTaskMicroseconds, "Time spend waiting for prefetched reader", 0) \
    \
    M(ThreadpoolReaderTaskMicroseconds, "Time spent getting the data in asynchronous reading", 0) \
    M(ThreadpoolReaderPrepareMicroseconds, "Time spent on preparation (e.g. call to reader seek() method)", 0) \
    M(ThreadpoolReaderReadBytes, "Bytes read from a threadpool task in asynchronous reading", 0) \
    M(ThreadpoolReaderSubmit, "Bytes read from a threadpool task in asynchronous reading", 0) \
    M(ThreadpoolReaderSubmitReadSynchronously, "How many times we haven't scheduled a task on the thread pool and read synchronously instead", 0) \
    M(ThreadpoolReaderSubmitReadSynchronouslyBytes, "How many bytes were read synchronously", 0) \
    M(ThreadpoolReaderSubmitReadSynchronouslyMicroseconds, "How much time we spent reading synchronously", 0) \
    M(ThreadpoolReaderSubmitLookupInCacheMicroseconds, "How much time we spent checking if content is cached", 0) \
    M(AsynchronousReaderIgnoredBytes, "Number of bytes ignored during asynchronous reading", 0) \
    \
    M(FileSegmentWaitReadBufferMicroseconds, "Metric per file segment. Time spend waiting for internal read buffer (includes cache waiting)", 0) \
    M(FileSegmentReadMicroseconds, "Metric per file segment. Time spend reading from file", 0) \
    M(FileSegmentCacheWriteMicroseconds, "Metric per file segment. Time spend writing data to cache", 0) \
    M(FileSegmentPredownloadMicroseconds, "Metric per file segment. Time spent pre-downloading data to cache (pre-downloading - finishing file segment download (after someone who failed to do that) up to the point current thread was requested to do)", 0) \
    M(FileSegmentUsedBytes, "Metric per file segment. How many bytes were actually used from current file segment", 0) \
    \
    M(ReadBufferSeekCancelConnection, "Number of seeks which lead to new connection (s3, http)", 0) \
    \
    M(SleepFunctionCalls, "Number of times a sleep function (sleep, sleepEachRow) has been called.", 0) \
    M(SleepFunctionMicroseconds, "Time set to sleep in a sleep function (sleep, sleepEachRow).", 0) \
    M(SleepFunctionElapsedMicroseconds, "Time spent sleeping in a sleep function (sleep, sleepEachRow).", 0) \
    \
    M(ThreadPoolReaderPageCacheHit, "Number of times the read inside ThreadPoolReader was done from the page cache.", 0) \
    M(ThreadPoolReaderPageCacheHitBytes, "Number of bytes read inside ThreadPoolReader when it was done from the page cache.", 0) \
    M(ThreadPoolReaderPageCacheHitElapsedMicroseconds, "Time spent reading data from page cache in ThreadPoolReader.", 0) \
    M(ThreadPoolReaderPageCacheMiss, "Number of times the read inside ThreadPoolReader was not done from page cache and was hand off to thread pool.", 0) \
    M(ThreadPoolReaderPageCacheMissBytes, "Number of bytes read inside ThreadPoolReader when read was not done from page cache and was hand off to thread pool.", 0) \
    M(ThreadPoolReaderPageCacheMissElapsedMicroseconds, "Time spent reading data inside the asynchronous job in ThreadPoolReader - when read was not done from the page cache.", 0) \
    \
    M(AsynchronousReadWaitMicroseconds, "Time spent in waiting for asynchronous reads in asynchronous local read.", 0) \
    M(SynchronousReadWaitMicroseconds, "Time spent in waiting for synchronous reads in asynchronous local read.", 0) \
    M(AsynchronousRemoteReadWaitMicroseconds, "Time spent in waiting for asynchronous remote reads.", 0) \
    M(SynchronousRemoteReadWaitMicroseconds, "Time spent in waiting for synchronous remote reads.", 0) \
    \
    M(ExternalDataSourceLocalCacheReadBytes, "Bytes read from local cache buffer in RemoteReadBufferCache", 0)\
    \
    M(MainConfigLoads, "Number of times the main configuration was reloaded.", 0) \
    \
    M(AggregationPreallocatedElementsInHashTables, "How many elements were preallocated in hash tables for aggregation.", 0) \
    M(AggregationHashTablesInitializedAsTwoLevel, "How many hash tables were inited as two-level for aggregation.", 0) \
    M(AggregationOptimizedEqualRangesOfKeys, "For how many blocks optimization of equal ranges of keys was applied", 0) \
    M(HashJoinPreallocatedElementsInHashTables, "How many elements were preallocated in hash tables for hash join.", 0) \
    \
    M(MetadataFromKeeperCacheHit, "Number of times an object storage metadata request was answered from cache without making request to Keeper", 0) \
    M(MetadataFromKeeperCacheMiss, "Number of times an object storage metadata request had to be answered from Keeper", 0) \
    M(MetadataFromKeeperCacheUpdateMicroseconds, "Total time spent in updating the cache including waiting for responses from Keeper", 0) \
    M(MetadataFromKeeperUpdateCacheOneLevel, "Number of times a cache update for one level of directory tree was done", 0) \
    M(MetadataFromKeeperTransactionCommit, "Number of times metadata transaction commit was attempted", 0) \
    M(MetadataFromKeeperTransactionCommitRetry, "Number of times metadata transaction commit was retried", 0) \
    M(MetadataFromKeeperCleanupTransactionCommit, "Number of times metadata transaction commit for deleted objects cleanup was attempted", 0) \
    M(MetadataFromKeeperCleanupTransactionCommitRetry, "Number of times metadata transaction commit for deleted objects cleanup was retried", 0) \
    M(MetadataFromKeeperOperations, "Number of times a request was made to Keeper", 0) \
    M(MetadataFromKeeperIndividualOperations, "Number of paths read or written by single or multi requests to Keeper", 0) \
    M(MetadataFromKeeperReconnects, "Number of times a reconnect to Keeper was done", 0) \
    M(MetadataFromKeeperBackgroundCleanupObjects, "Number of times a old deleted object clean up was performed by background task", 0) \
    M(MetadataFromKeeperBackgroundCleanupTransactions, "Number of times old transaction idempotency token was cleaned up by background task", 0) \
    M(MetadataFromKeeperBackgroundCleanupErrors, "Number of times an error was encountered in background cleanup task", 0) \
    \
    M(KafkaRebalanceRevocations, "Number of partition revocations (the first stage of consumer group rebalance)", 0) \
    M(KafkaRebalanceAssignments, "Number of partition assignments (the final stage of consumer group rebalance)", 0) \
    M(KafkaRebalanceErrors, "Number of failed consumer group rebalances", 0) \
    M(KafkaMessagesPolled, "Number of Kafka messages polled from librdkafka to ClickHouse", 0) \
    M(KafkaMessagesRead, "Number of Kafka messages already processed by ClickHouse", 0) \
    M(KafkaMessagesFailed, "Number of Kafka messages ClickHouse failed to parse", 0) \
    M(KafkaRowsRead, "Number of rows parsed from Kafka messages", 0) \
    M(KafkaRowsRejected, "Number of parsed rows which were later rejected (due to rebalances / errors or similar reasons). Those rows will be consumed again after the rebalance.", 0) \
    M(KafkaDirectReads, "Number of direct selects from Kafka tables since server start", 0) \
    M(KafkaBackgroundReads, "Number of background reads populating materialized views from Kafka since server start", 0) \
    M(KafkaCommits, "Number of successful commits of consumed offsets to Kafka (normally should be the same as KafkaBackgroundReads)", 0) \
    M(KafkaCommitFailures, "Number of failed commits of consumed offsets to Kafka (usually is a sign of some data duplication)", 0) \
    M(KafkaConsumerErrors, "Number of errors reported by librdkafka during polls", 0) \
    M(KafkaWrites, "Number of writes (inserts) to Kafka tables ", 0) \
    M(KafkaRowsWritten, "Number of rows inserted into Kafka tables", 0) \
    M(KafkaProducerFlushes, "Number of explicit flushes to Kafka producer", 0) \
    M(KafkaMessagesProduced, "Number of messages produced to Kafka", 0) \
    M(KafkaProducerErrors, "Number of errors during producing the messages to Kafka", 0) \
    \
    M(ScalarSubqueriesGlobalCacheHit, "Number of times a read from a scalar subquery was done using the global cache", 0) \
    M(ScalarSubqueriesLocalCacheHit, "Number of times a read from a scalar subquery was done using the local cache", 0) \
    M(ScalarSubqueriesCacheMiss, "Number of times a read from a scalar subquery was not cached and had to be calculated completely", 0)                                                                                                                                                                                                 \
    \
    M(SchemaInferenceCacheHits, "Number of times the requested source is found in schema cache", 0) \
    M(SchemaInferenceCacheSchemaHits, "Number of times the schema is found in schema cache during schema inference", 0) \
    M(SchemaInferenceCacheNumRowsHits, "Number of times the number of rows is found in schema cache during count from files", 0) \
    M(SchemaInferenceCacheMisses, "Number of times the requested source is not in schema cache", 0) \
    M(SchemaInferenceCacheSchemaMisses, "Number of times the requested source is in cache but the schema is not in cache during schema inference", 0) \
    M(SchemaInferenceCacheNumRowsMisses, "Number of times the requested source is in cache but the number of rows is not in cache while count from files", 0) \
    M(SchemaInferenceCacheEvictions, "Number of times a schema from cache was evicted due to overflow", 0) \
    M(SchemaInferenceCacheInvalidations, "Number of times a schema in cache became invalid due to changes in data", 0) \
    \
    M(KeeperPacketsSent, "Packets sent by keeper server", 0) \
    M(KeeperPacketsReceived, "Packets received by keeper server", 0) \
    M(KeeperRequestTotal, "Total requests number on keeper server", 0) \
    M(KeeperLatency, "Keeper latency", 0) \
    M(KeeperTotalElapsedMicroseconds, "Keeper total latency for a single request", 0) \
    M(KeeperProcessElapsedMicroseconds, "Keeper commit latency for a single request", 0) \
    M(KeeperPreprocessElapsedMicroseconds, "Keeper preprocessing latency for a single reuquest", 0) \
    M(KeeperStorageLockWaitMicroseconds, "Time spent waiting for acquiring Keeper storage lock", 0) \
    M(KeeperCommitWaitElapsedMicroseconds, "Time spent waiting for certain log to be committed", 0) \
    M(KeeperBatchMaxCount, "Number of times the size of batch was limited by the amount", 0) \
    M(KeeperBatchMaxTotalSize, "Number of times the size of batch was limited by the total bytes size", 0) \
    M(KeeperCommits, "Number of successful commits", 0) \
    M(KeeperCommitsFailed, "Number of failed commits", 0) \
    M(KeeperSnapshotCreations, "Number of snapshots creations", 0)\
    M(KeeperSnapshotCreationsFailed, "Number of failed snapshot creations", 0)\
    M(KeeperSnapshotApplys, "Number of snapshot applying", 0)\
    M(KeeperSnapshotApplysFailed, "Number of failed snapshot applying", 0)\
    M(KeeperReadSnapshot, "Number of snapshot read(serialization)", 0)\
    M(KeeperSaveSnapshot, "Number of snapshot save", 0)\
    M(KeeperCreateRequest, "Number of create requests", 0)\
    M(KeeperRemoveRequest, "Number of remove requests", 0)\
    M(KeeperSetRequest, "Number of set requests", 0)\
    M(KeeperReconfigRequest, "Number of reconfig requests", 0)\
    M(KeeperCheckRequest, "Number of check requests", 0)\
    M(KeeperMultiRequest, "Number of multi requests", 0)\
    M(KeeperMultiReadRequest, "Number of multi read requests", 0)\
    M(KeeperGetRequest, "Number of get requests", 0)\
    M(KeeperListRequest, "Number of list requests", 0)\
    M(KeeperExistsRequest, "Number of exists requests", 0)\
    \
    M(OverflowBreak, "Number of times, data processing was cancelled by query complexity limitation with setting '*_overflow_mode' = 'break' and the result is incomplete.", 0) \
    M(OverflowThrow, "Number of times, data processing was cancelled by query complexity limitation with setting '*_overflow_mode' = 'throw' and exception was thrown.", 0) \
    M(OverflowAny, "Number of times approximate GROUP BY was in effect: when aggregation was performed only on top of first 'max_rows_to_group_by' unique keys and other keys were ignored due to 'group_by_overflow_mode' = 'any'.", 0) \
    \
    M(S3QueueSetFileProcessingMicroseconds, "Time spent to set file as processing", 0)\
    M(S3QueueSetFileProcessedMicroseconds, "Time spent to set file as processed", 0)\
    M(S3QueueSetFileFailedMicroseconds, "Time spent to set file as failed", 0)\
    M(ObjectStorageQueueFailedFiles, "Number of files which failed to be processed", 0)\
    M(ObjectStorageQueueProcessedFiles, "Number of files which were processed", 0)\
    M(ObjectStorageQueueCleanupMaxSetSizeOrTTLMicroseconds, "Time spent to set file as failed", 0)\
    M(ObjectStorageQueuePullMicroseconds, "Time spent to read file data", 0)\
    M(ObjectStorageQueueLockLocalFileStatusesMicroseconds, "Time spent to lock local file statuses", 0)\
    \
    M(ServerStartupMilliseconds, "Time elapsed from starting server to listening to sockets in milliseconds", 0)\
    M(IOUringSQEsSubmitted, "Total number of io_uring SQEs submitted", 0) \
    M(IOUringSQEsResubmitsAsync, "Total number of asynchronous io_uring SQE resubmits performed", 0) \
    M(IOUringSQEsResubmitsSync, "Total number of synchronous io_uring SQE resubmits performed", 0) \
    M(IOUringCQEsCompleted, "Total number of successfully completed io_uring CQEs", 0) \
    M(IOUringCQEsFailed, "Total number of completed io_uring CQEs with failures", 0) \
    \
    M(BackupsOpenedForRead, "Number of backups opened for reading", 0) \
    M(BackupsOpenedForWrite, "Number of backups opened for writing", 0) \
    M(BackupReadMetadataMicroseconds, "Time spent reading backup metadata from .backup file", 0) \
    M(BackupWriteMetadataMicroseconds, "Time spent writing backup metadata to .backup file", 0) \
    M(BackupEntriesCollectorMicroseconds, "Time spent making backup entries", 0) \
    M(BackupEntriesCollectorForTablesDataMicroseconds, "Time spent making backup entries for tables data", 0) \
    M(BackupEntriesCollectorRunPostTasksMicroseconds, "Time spent running post tasks after making backup entries", 0) \
    \
    M(ReadTaskRequestsReceived, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the initiator server side.", 0) \
    M(MergeTreeReadTaskRequestsReceived, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the initiator server side.", 0) \
    \
    M(ReadTaskRequestsSent, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the remote server side.", 0) \
    M(MergeTreeReadTaskRequestsSent, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the remote server side.", 0) \
    M(MergeTreeAllRangesAnnouncementsSent, "The number of announcements sent from the remote server to the initiator server about the set of data parts (for MergeTree tables). Measured on the remote server side.", 0) \
    M(ReadTaskRequestsSentElapsedMicroseconds, "Time spent in callbacks requested from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the remote server side.", 0) \
    M(MergeTreeReadTaskRequestsSentElapsedMicroseconds, "Time spent in callbacks requested from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the remote server side.", 0) \
    M(MergeTreeAllRangesAnnouncementsSentElapsedMicroseconds, "Time spent in sending the announcement from the remote server to the initiator server about the set of data parts (for MergeTree tables). Measured on the remote server side.", 0) \
    \
    M(ConnectionPoolIsFullMicroseconds, "Total time spent waiting for a slot in connection pool.", 0) \
    M(AsyncLoaderWaitMicroseconds, "Total time a query was waiting for async loader jobs.", 0) \
    \
    M(DistrCacheServerSwitches, "Number of server switches between distributed cache servers in read/write-through cache", 0) \
    M(DistrCacheReadMicroseconds, "Time spent reading from distributed cache", 0) \
    M(DistrCacheFallbackReadMicroseconds, "Time spend reading from fallback buffer instead of distribted cache", 0) \
    M(DistrCachePrecomputeRangesMicroseconds, "Time spent to precompute read ranges", 0) \
    M(DistrCacheNextImplMicroseconds, "Time spend in ReadBufferFromDistributedCache::nextImpl", 0) \
    M(DistrCacheOpenedConnections, "The number of open connections to distributed cache", 0) \
    M(DistrCacheReusedConnections, "The number of reused connections to distributed cache", 0) \
    M(DistrCacheHoldConnections, "The number of used connections to distributed cache", 0) \
    \
    M(DistrCacheGetResponseMicroseconds, "Time spend to wait for response from distributed cache", 0) \
    M(DistrCacheStartRangeMicroseconds, "Time spent to start a new read range with distributed cache", 0) \
    M(DistrCacheLockRegistryMicroseconds, "Time spent to take DistributedCacheRegistry lock", 0) \
    M(DistrCacheUnusedPackets, "Number of skipped unused packets from distributed cache", 0) \
    M(DistrCachePackets, "Total number of packets received from distributed cache", 0) \
    M(DistrCacheUnusedPacketsBytes, "The number of bytes in Data packets which were ignored", 0) \
    M(DistrCacheRegistryUpdateMicroseconds, "Time spent updating distributed cache registry", 0) \
    M(DistrCacheRegistryUpdates, "Number of distributed cache registry updates", 0) \
    \
    M(DistrCacheConnectMicroseconds, "The time spent to connect to distributed cache", 0) \
    M(DistrCacheConnectAttempts, "The number of connection attempts to distributed cache", 0) \
    M(DistrCacheGetClient, "Number of client access times", 0) \
    \
    M(DistrCacheServerProcessRequestMicroseconds, "Time spent processing request on DistributedCache server side", 0) \
    \
    M(LogTest, "Number of log messages with level Test", 0) \
    M(LogTrace, "Number of log messages with level Trace", 0) \
    M(LogDebug, "Number of log messages with level Debug", 0) \
    M(LogInfo, "Number of log messages with level Info", 0) \
    M(LogWarning, "Number of log messages with level Warning", 0) \
    M(LogError, "Number of log messages with level Error", 0) \
    M(LogFatal, "Number of log messages with level Fatal", 0) \
    \
    M(InterfaceHTTPSendBytes, "Number of bytes sent through HTTP interfaces", 0) \
    M(InterfaceHTTPReceiveBytes, "Number of bytes received through HTTP interfaces", 0) \
    M(InterfaceNativeSendBytes, "Number of bytes sent through native interfaces", 0) \
    M(InterfaceNativeReceiveBytes, "Number of bytes received through native interfaces", 0) \
    M(InterfacePrometheusSendBytes, "Number of bytes sent through Prometheus interfaces", 0) \
    M(InterfacePrometheusReceiveBytes, "Number of bytes received through Prometheus interfaces", 0) \
    M(InterfaceInterserverSendBytes, "Number of bytes sent through interserver interfaces", 0) \
    M(InterfaceInterserverReceiveBytes, "Number of bytes received through interserver interfaces", 0) \
    M(InterfaceMySQLSendBytes, "Number of bytes sent through MySQL interfaces", 0) \
    M(InterfaceMySQLReceiveBytes, "Number of bytes received through MySQL interfaces", 0) \
    M(InterfacePostgreSQLSendBytes, "Number of bytes sent through PostgreSQL interfaces", 0) \
    M(InterfacePostgreSQLReceiveBytes, "Number of bytes received through PostgreSQL interfaces", 0) \
    \
    M(ParallelReplicasUsedCount, "Number of replicas used to execute a query with task-based parallel replicas", 0) \
    \
    M(KeeperLogsEntryReadFromLatestCache, "Number of log entries in Keeper being read from latest logs cache", 0) \
    M(KeeperLogsEntryReadFromCommitCache, "Number of log entries in Keeper being read from commit logs cache", 0) \
    M(KeeperLogsEntryReadFromFile, "Number of log entries in Keeper being read directly from the changelog file", 0) \
    M(KeeperLogsPrefetchedEntries, "Number of log entries in Keeper being prefetched from the changelog file", 0) \
    \
    M(ParallelReplicasAvailableCount, "Number of replicas available to execute a query with task-based parallel replicas", 0) \
    M(ParallelReplicasUnavailableCount, "Number of replicas which was chosen, but found to be unavailable during query execution with task-based parallel replicas", 0) \
    \
    M(StorageConnectionsCreated, "Number of created connections for storages", 0) \
    M(StorageConnectionsReused, "Number of reused connections for storages", 0) \
    M(StorageConnectionsReset, "Number of reset connections for storages", 0) \
    M(StorageConnectionsPreserved, "Number of preserved connections for storages", 0) \
    M(StorageConnectionsExpired, "Number of expired connections for storages", 0) \
    M(StorageConnectionsErrors, "Number of cases when creation of a connection for storage is failed", 0) \
    M(StorageConnectionsElapsedMicroseconds, "Total time spend on creating connections for storages", 0)                                                                                                                                                                                                                                               \
    \
    M(DiskConnectionsCreated, "Number of created connections for disk", 0) \
    M(DiskConnectionsReused, "Number of reused connections for disk", 0) \
    M(DiskConnectionsReset, "Number of reset connections for disk", 0) \
    M(DiskConnectionsPreserved, "Number of preserved connections for disk", 0) \
    M(DiskConnectionsExpired, "Number of expired connections for disk", 0) \
    M(DiskConnectionsErrors, "Number of cases when creation of a connection for disk is failed", 0) \
    M(DiskConnectionsElapsedMicroseconds, "Total time spend on creating connections for disk", 0) \
    \
    M(HTTPConnectionsCreated, "Number of created http connections", 0) \
    M(HTTPConnectionsReused, "Number of reused http connections", 0) \
    M(HTTPConnectionsReset, "Number of reset http connections", 0) \
    M(HTTPConnectionsPreserved, "Number of preserved http connections", 0) \
    M(HTTPConnectionsExpired, "Number of expired http connections", 0) \
    M(HTTPConnectionsErrors, "Number of cases when creation of a http connection failed", 0) \
    M(HTTPConnectionsElapsedMicroseconds, "Total time spend on creating http connections", 0) \
    \
    M(AddressesDiscovered, "Total count of new addresses in dns resolve results for http connections", 0) \
    M(AddressesExpired, "Total count of expired addresses which is no longer presented in dns resolve results for http connections", 0) \
    M(AddressesMarkedAsFailed, "Total count of addresses which has been marked as faulty due to connection errors for http connections", 0) \
    \
    M(ReadWriteBufferFromHTTPRequestsSent, "Number of HTTP requests sent by ReadWriteBufferFromHTTP", 0) \
    M(ReadWriteBufferFromHTTPBytes, "Total size of payload bytes received and sent by ReadWriteBufferFromHTTP. Doesn't include HTTP headers.", 0) \
    \
    M(GWPAsanAllocateSuccess, "Number of successful allocations done by GWPAsan", 0) \
    M(GWPAsanAllocateFailed, "Number of failed allocations done by GWPAsan (i.e. filled pool)", 0) \
    M(GWPAsanFree, "Number of free operations done by GWPAsan", 0) \
    \
    M(MemoryWorkerRun, "Number of runs done by MemoryWorker in background", 0) \
    M(MemoryWorkerRunElapsedMicroseconds, "Total time spent by MemoryWorker for background work", 0) \


#ifdef APPLY_FOR_EXTERNAL_EVENTS
    #define APPLY_FOR_EVENTS(M) APPLY_FOR_BUILTIN_EVENTS(M) APPLY_FOR_EXTERNAL_EVENTS(M)
#else
    #define APPLY_FOR_EVENTS(M) APPLY_FOR_BUILTIN_EVENTS(M)
#endif

namespace ProfileEvents
{

#define M(NAME, DOCUMENTATION, ...) extern const Event NAME = Event(__COUNTER__);
    APPLY_FOR_EVENTS(M)
#undef M
constexpr Event END = Event(__COUNTER__);

/// Global variable, initialized by zeros.
Counter global_counters_array[END] {};
/// Global variable, initialized by zeros.
Counter global_last_values_array[END] {};
/// Global variable, initialized by zeros.
#define M(NAME, DOCUMENTATION, WINDOW_SIZE) std::vector<Counter>(WINDOW_SIZE),
    std::vector<Counter> global_window_array[END] {APPLY_FOR_EVENTS(M)};
#undef M
/// Initialize global counters statically
Counters global_counters(global_counters_array, global_last_values_array, global_window_array);

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
    parent = nullptr;
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
    #define M(NAME, DOCUMENTATION, ...) #NAME,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

const char * getDocumentation(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION, ...) DOCUMENTATION,
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

void incrementNoTrace(Event event, Count amount)
{
    DB::CurrentThread::getProfileEvents().incrementNoTrace(event, amount);
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
