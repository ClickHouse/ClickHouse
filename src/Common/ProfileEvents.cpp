#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <Common/TraceSender.h>


// clang-format off
/// Available events. Add something here as you wish.
/// If the event is generic (i.e. not server specific)
/// it should be also added to src/Coordination/KeeperConstant.cpp
#define APPLY_FOR_BUILTIN_EVENTS(M) \
    M(Query, "Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.") \
    M(SelectQuery, "Same as Query, but only for SELECT queries.") \
    M(InsertQuery, "Same as Query, but only for INSERT queries.") \
    M(InitialQuery, "Same as Query, but only counts initial queries (see is_initial_query).")\
    M(QueriesWithSubqueries, "Count queries with all subqueries") \
    M(SelectQueriesWithSubqueries, "Count SELECT queries with all subqueries") \
    M(InsertQueriesWithSubqueries, "Count INSERT queries with all subqueries") \
    M(SelectQueriesWithPrimaryKeyUsage, "Count SELECT queries which use the primary key to evaluate the WHERE condition") \
    M(AsyncInsertQuery, "Same as InsertQuery, but only for asynchronous INSERT queries.") \
    M(AsyncInsertBytes, "Data size in bytes of asynchronous INSERT queries.") \
    M(AsyncInsertRows, "Number of rows inserted by asynchronous INSERT queries.") \
    M(AsyncInsertCacheHits, "Number of times a duplicate hash id has been found in asynchronous INSERT hash id cache.") \
    M(FailedQuery, "Number of failed queries.") \
    M(FailedSelectQuery, "Same as FailedQuery, but only for SELECT queries.") \
    M(FailedInsertQuery, "Same as FailedQuery, but only for INSERT queries.") \
    M(FailedAsyncInsertQuery, "Number of failed ASYNC INSERT queries.") \
    M(QueryTimeMicroseconds, "Total time of all queries.") \
    M(SelectQueryTimeMicroseconds, "Total time of SELECT queries.") \
    M(InsertQueryTimeMicroseconds, "Total time of INSERT queries.") \
    M(OtherQueryTimeMicroseconds, "Total time of queries that are not SELECT or INSERT.") \
    M(FileOpen, "Number of files opened.") \
    M(Seek, "Number of times the 'lseek' function was called.") \
    M(ReadBufferFromFileDescriptorRead, "Number of reads (read/pread) from a file descriptor. Does not include sockets.") \
    M(ReadBufferFromFileDescriptorReadFailed, "Number of times the read (read/pread) from a file descriptor have failed.") \
    M(ReadBufferFromFileDescriptorReadBytes, "Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.") \
    M(WriteBufferFromFileDescriptorWrite, "Number of writes (write/pwrite) to a file descriptor. Does not include sockets.") \
    M(WriteBufferFromFileDescriptorWriteFailed, "Number of times the write (write/pwrite) to a file descriptor have failed.") \
    M(WriteBufferFromFileDescriptorWriteBytes, "Number of bytes written to file descriptors. If the file is compressed, this will show compressed data size.") \
    M(FileSync, "Number of times the F_FULLFSYNC/fsync/fdatasync function was called for files.") \
    M(DirectorySync, "Number of times the F_FULLFSYNC/fsync/fdatasync function was called for directories.") \
    M(FileSyncElapsedMicroseconds, "Total time spent waiting for F_FULLFSYNC/fsync/fdatasync syscall for files.") \
    M(DirectorySyncElapsedMicroseconds, "Total time spent waiting for F_FULLFSYNC/fsync/fdatasync syscall for directories.") \
    M(ReadCompressedBytes, "Number of bytes (the number of bytes before decompression) read from compressed sources (files, network).") \
    M(CompressedReadBufferBlocks, "Number of compressed blocks (the blocks of data that are compressed independent of each other) read from compressed sources (files, network).") \
    M(CompressedReadBufferBytes, "Number of uncompressed bytes (the number of bytes after decompression) read from compressed sources (files, network).") \
    M(UncompressedCacheHits, "Number of times a block of data has been found in the uncompressed cache (and decompression was avoided).") \
    M(UncompressedCacheMisses, "Number of times a block of data has not been found in the uncompressed cache (and required decompression).") \
    M(UncompressedCacheWeightLost, "Number of bytes evicted from the uncompressed cache.") \
    M(MMappedFileCacheHits, "Number of times a file has been found in the MMap cache (for the 'mmap' read_method), so we didn't have to mmap it again.") \
    M(MMappedFileCacheMisses, "Number of times a file has not been found in the MMap cache (for the 'mmap' read_method), so we had to mmap it again.") \
    M(OpenedFileCacheHits, "Number of times a file has been found in the opened file cache, so we didn't have to open it again.") \
    M(OpenedFileCacheMisses, "Number of times a file has been found in the opened file cache, so we had to open it again.") \
    M(OpenedFileCacheMicroseconds, "Amount of time spent executing OpenedFileCache methods.") \
    M(AIOWrite, "Number of writes with Linux or FreeBSD AIO interface") \
    M(AIOWriteBytes, "Number of bytes written with Linux or FreeBSD AIO interface") \
    M(AIORead, "Number of reads with Linux or FreeBSD AIO interface") \
    M(AIOReadBytes, "Number of bytes read with Linux or FreeBSD AIO interface") \
    M(IOBufferAllocs, "Number of allocations of IO buffers (for ReadBuffer/WriteBuffer).") \
    M(IOBufferAllocBytes, "Number of bytes allocated for IO buffers (for ReadBuffer/WriteBuffer).") \
    M(ArenaAllocChunks, "Number of chunks allocated for memory Arena (used for GROUP BY and similar operations)") \
    M(ArenaAllocBytes, "Number of bytes allocated for memory Arena (used for GROUP BY and similar operations)") \
    M(FunctionExecute, "Number of SQL ordinary function calls (SQL functions are called on per-block basis, so this number represents the number of blocks).") \
    M(TableFunctionExecute, "Number of table function calls.") \
    M(MarkCacheHits, "Number of times an entry has been found in the mark cache, so we didn't have to load a mark file.") \
    M(MarkCacheMisses, "Number of times an entry has not been found in the mark cache, so we had to load a mark file in memory, which is a costly operation, adding to query latency.") \
    M(QueryCacheHits, "Number of times a query result has been found in the query cache (and query computation was avoided). Only updated for SELECT queries with SETTING use_query_cache = 1.") \
    M(QueryCacheMisses, "Number of times a query result has not been found in the query cache (and required query computation). Only updated for SELECT queries with SETTING use_query_cache = 1.") \
    /* Each page cache chunk access increments exactly one of the following 5 PageCacheChunk* counters. */ \
    /* Something like hit rate: (PageCacheChunkShared + PageCacheChunkDataHits) / [sum of all 5]. */ \
    M(PageCacheChunkMisses, "Number of times a chunk has not been found in the userspace page cache.") \
    M(PageCacheChunkShared, "Number of times a chunk has been found in the userspace page cache, already in use by another thread.") \
    M(PageCacheChunkDataHits, "Number of times a chunk has been found in the userspace page cache, not in use, with all pages intact.") \
    M(PageCacheChunkDataPartialHits, "Number of times a chunk has been found in the userspace page cache, not in use, but some of its pages were evicted by the OS.") \
    M(PageCacheChunkDataMisses, "Number of times a chunk has been found in the userspace page cache, not in use, but all its pages were evicted by the OS.") \
    M(PageCacheBytesUnpinnedRoundedToPages, "Total size of populated pages in chunks that became evictable in PageCache. Rounded up to whole pages.") \
    M(PageCacheBytesUnpinnedRoundedToHugePages, "See PageCacheBytesUnpinnedRoundedToPages, but rounded to huge pages. Use the ratio between the two as a measure of memory waste from using huge pages.") \
    M(CreatedReadBufferOrdinary, "Number of times ordinary read buffer was created for reading data (while choosing among other read methods).") \
    M(CreatedReadBufferDirectIO, "Number of times a read buffer with O_DIRECT was created for reading data (while choosing among other read methods).") \
    M(CreatedReadBufferDirectIOFailed, "Number of times a read buffer with O_DIRECT was attempted to be created for reading data (while choosing among other read methods), but the OS did not allow it (due to lack of filesystem support or other reasons) and we fallen back to the ordinary reading method.") \
    M(CreatedReadBufferMMap, "Number of times a read buffer using 'mmap' was created for reading data (while choosing among other read methods).") \
    M(CreatedReadBufferMMapFailed, "Number of times a read buffer with 'mmap' was attempted to be created for reading data (while choosing among other read methods), but the OS did not allow it (due to lack of filesystem support or other reasons) and we fallen back to the ordinary reading method.") \
    M(DiskReadElapsedMicroseconds, "Total time spent waiting for read syscall. This include reads from page cache.") \
    M(DiskWriteElapsedMicroseconds, "Total time spent waiting for write syscall. This include writes to page cache.") \
    M(NetworkReceiveElapsedMicroseconds, "Total time spent waiting for data to receive or receiving data from network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.") \
    M(NetworkSendElapsedMicroseconds, "Total time spent waiting for data to send to network or sending data to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.") \
    M(NetworkReceiveBytes, "Total number of bytes received from network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.") \
    M(NetworkSendBytes, "Total number of bytes send to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.") \
    \
    M(DiskS3GetRequestThrottlerCount, "Number of DiskS3 GET and SELECT requests passed through throttler.") \
    M(DiskS3GetRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform DiskS3 GET and SELECT request throttling.") \
    M(DiskS3PutRequestThrottlerCount, "Number of DiskS3 PUT, COPY, POST and LIST requests passed through throttler.") \
    M(DiskS3PutRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform DiskS3 PUT, COPY, POST and LIST request throttling.") \
    M(S3GetRequestThrottlerCount, "Number of S3 GET and SELECT requests passed through throttler.") \
    M(S3GetRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform S3 GET and SELECT request throttling.") \
    M(S3PutRequestThrottlerCount, "Number of S3 PUT, COPY, POST and LIST requests passed through throttler.") \
    M(S3PutRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform S3 PUT, COPY, POST and LIST request throttling.") \
    M(RemoteReadThrottlerBytes, "Bytes passed through 'max_remote_read_network_bandwidth_for_server'/'max_remote_read_network_bandwidth' throttler.") \
    M(RemoteReadThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_remote_read_network_bandwidth_for_server'/'max_remote_read_network_bandwidth' throttling.") \
    M(RemoteWriteThrottlerBytes, "Bytes passed through 'max_remote_write_network_bandwidth_for_server'/'max_remote_write_network_bandwidth' throttler.") \
    M(RemoteWriteThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_remote_write_network_bandwidth_for_server'/'max_remote_write_network_bandwidth' throttling.") \
    M(LocalReadThrottlerBytes, "Bytes passed through 'max_local_read_bandwidth_for_server'/'max_local_read_bandwidth' throttler.") \
    M(LocalReadThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_local_read_bandwidth_for_server'/'max_local_read_bandwidth' throttling.") \
    M(LocalWriteThrottlerBytes, "Bytes passed through 'max_local_write_bandwidth_for_server'/'max_local_write_bandwidth' throttler.") \
    M(LocalWriteThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_local_write_bandwidth_for_server'/'max_local_write_bandwidth' throttling.") \
    M(ThrottlerSleepMicroseconds, "Total time a query was sleeping to conform all throttling settings.") \
    M(PartsWithAppliedMutationsOnFly, "Total number of parts for which there was any mutation applied on fly") \
    M(MutationsAppliedOnFlyInAllParts, "The sum of number of applied mutations on-fly for part among all read parts") \
    \
    M(QueryMaskingRulesMatch, "Number of times query masking rules was successfully matched.") \
    \
    M(ReplicatedPartFetches, "Number of times a data part was downloaded from replica of a ReplicatedMergeTree table.") \
    M(ReplicatedPartFailedFetches, "Number of times a data part was failed to download from replica of a ReplicatedMergeTree table.") \
    M(ObsoleteReplicatedParts, "Number of times a data part was covered by another data part that has been fetched from a replica (so, we have marked a covered data part as obsolete and no longer needed).") \
    M(ReplicatedPartMerges, "Number of times data parts of ReplicatedMergeTree tables were successfully merged.") \
    M(ReplicatedPartFetchesOfMerged, "Number of times we prefer to download already merged part from replica of ReplicatedMergeTree table instead of performing a merge ourself (usually we prefer doing a merge ourself to save network traffic). This happens when we have not all source parts to perform a merge or when the data part is old enough.") \
    M(ReplicatedPartMutations, "Number of times data parts of ReplicatedMergeTree tables were successfully mutated.") \
    M(ReplicatedPartChecks, "Number of times we had to perform advanced search for a data part on replicas or to clarify the need of an existing data part.") \
    M(ReplicatedPartChecksFailed, "Number of times the advanced search for a data part on replicas did not give result or when unexpected part has been found and moved away.") \
    M(ReplicatedDataLoss, "Number of times a data part that we wanted doesn't exist on any replica (even on replicas that are offline right now). That data parts are definitely lost. This is normal due to asynchronous replication (if quorum inserts were not enabled), when the replica on which the data part was written was failed and when it became online after fail it doesn't contain that data part.") \
    M(ReplicatedCoveredPartsInZooKeeperOnStart, "For debugging purposes. Number of parts in ZooKeeper that have a covering part, but doesn't exist on disk. Checked on server start.") \
    \
    M(InsertedRows, "Number of rows INSERTed to all tables.") \
    M(InsertedBytes, "Number of bytes (uncompressed; for columns as they stored in memory) INSERTed to all tables.") \
    M(DelayedInserts, "Number of times the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.") \
    M(RejectedInserts, "Number of times the INSERT of a block to a MergeTree table was rejected with 'Too many parts' exception due to high number of active data parts for partition.") \
    M(DelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.") \
    M(DelayedMutations, "Number of times the mutation of a MergeTree table was throttled due to high number of unfinished mutations for table.") \
    M(RejectedMutations, "Number of times the mutation of a MergeTree table was rejected with 'Too many mutations' exception due to high number of unfinished mutations for table.") \
    M(DelayedMutationsMilliseconds, "Total number of milliseconds spent while the mutation of a MergeTree table was throttled due to high number of unfinished mutations for table.") \
    M(DistributedDelayedInserts, "Number of times the INSERT of a block to a Distributed table was throttled due to high number of pending bytes.") \
    M(DistributedRejectedInserts, "Number of times the INSERT of a block to a Distributed table was rejected with 'Too many bytes' exception due to high number of pending bytes.") \
    M(DistributedDelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a Distributed table was throttled due to high number of pending bytes.") \
    M(DuplicatedInsertedBlocks, "Number of times the INSERTed block to a ReplicatedMergeTree table was deduplicated.") \
    \
    M(ZooKeeperInit, "Number of times connection with ZooKeeper has been established.") \
    M(ZooKeeperTransactions, "Number of ZooKeeper operations, which include both read and write operations as well as multi-transactions.") \
    M(ZooKeeperList, "Number of 'list' (getChildren) requests to ZooKeeper.") \
    M(ZooKeeperCreate, "Number of 'create' requests to ZooKeeper.") \
    M(ZooKeeperRemove, "Number of 'remove' requests to ZooKeeper.") \
    M(ZooKeeperExists, "Number of 'exists' requests to ZooKeeper.") \
    M(ZooKeeperGet, "Number of 'get' requests to ZooKeeper.") \
    M(ZooKeeperSet, "Number of 'set' requests to ZooKeeper.") \
    M(ZooKeeperMulti, "Number of 'multi' requests to ZooKeeper (compound transactions).") \
    M(ZooKeeperCheck, "Number of 'check' requests to ZooKeeper. Usually they don't make sense in isolation, only as part of a complex transaction.") \
    M(ZooKeeperSync, "Number of 'sync' requests to ZooKeeper. These requests are rarely needed or usable.") \
    M(ZooKeeperReconfig, "Number of 'reconfig' requests to ZooKeeper.") \
    M(ZooKeeperClose, "Number of times connection with ZooKeeper has been closed voluntary.") \
    M(ZooKeeperWatchResponse, "Number of times watch notification has been received from ZooKeeper.") \
    M(ZooKeeperUserExceptions, "Number of exceptions while working with ZooKeeper related to the data (no node, bad version or similar).") \
    M(ZooKeeperHardwareExceptions, "Number of exceptions while working with ZooKeeper related to network (connection loss or similar).") \
    M(ZooKeeperOtherExceptions, "Number of exceptions while working with ZooKeeper other than ZooKeeperUserExceptions and ZooKeeperHardwareExceptions.") \
    M(ZooKeeperWaitMicroseconds, "Number of microseconds spent waiting for responses from ZooKeeper after creating a request, summed across all the requesting threads.") \
    M(ZooKeeperBytesSent, "Number of bytes send over network while communicating with ZooKeeper.") \
    M(ZooKeeperBytesReceived, "Number of bytes received over network while communicating with ZooKeeper.") \
    \
    M(DistributedConnectionTries, "Total count of distributed connection attempts.") \
    M(DistributedConnectionUsable, "Total count of successful distributed connections to a usable server (with required table, but maybe stale).") \
    M(DistributedConnectionFailTry, "Total count when distributed connection fails with retry.") \
    M(DistributedConnectionMissingTable, "Number of times we rejected a replica from a distributed query, because it did not contain a table needed for the query.") \
    M(DistributedConnectionStaleReplica, "Number of times we rejected a replica from a distributed query, because some table needed for a query had replication lag higher than the configured threshold.") \
    M(DistributedConnectionSkipReadOnlyReplica, "Number of replicas skipped during INSERT into Distributed table due to replicas being read-only") \
    M(DistributedConnectionFailAtAll, "Total count when distributed connection fails after all retries finished.") \
    \
    M(HedgedRequestsChangeReplica, "Total count when timeout for changing replica expired in hedged requests.") \
    M(SuspendSendingQueryToShard, "Total count when sending query to shard was suspended when async_query_sending_for_remote is enabled.") \
    \
    M(CompileFunction, "Number of times a compilation of generated LLVM code (to create fused function for complex expressions) was initiated.") \
    M(CompiledFunctionExecute, "Number of times a compiled function was executed.") \
    M(CompileExpressionsMicroseconds, "Total time spent for compilation of expressions to LLVM code.") \
    M(CompileExpressionsBytes, "Number of bytes used for expressions compilation.") \
    \
    M(ExecuteShellCommand, "Number of shell command executions.") \
    \
    M(ExternalProcessingCompressedBytesTotal, "Number of compressed bytes written by external processing (sorting/aggragating/joining)") \
    M(ExternalProcessingUncompressedBytesTotal, "Amount of data (uncompressed, before compression) written by external processing (sorting/aggragating/joining)") \
    M(ExternalProcessingFilesTotal, "Number of files used by external processing (sorting/aggragating/joining)") \
    M(ExternalSortWritePart, "Number of times a temporary file was written to disk for sorting in external memory.") \
    M(ExternalSortMerge, "Number of times temporary files were merged for sorting in external memory.") \
    M(ExternalSortCompressedBytes, "Number of compressed bytes written for sorting in external memory.") \
    M(ExternalSortUncompressedBytes, "Amount of data (uncompressed, before compression) written for sorting in external memory.") \
    M(ExternalAggregationWritePart, "Number of times a temporary file was written to disk for aggregation in external memory.") \
    M(ExternalAggregationMerge, "Number of times temporary files were merged for aggregation in external memory.") \
    M(ExternalAggregationCompressedBytes, "Number of bytes written to disk for aggregation in external memory.") \
    M(ExternalAggregationUncompressedBytes, "Amount of data (uncompressed, before compression) written to disk for aggregation in external memory.") \
    M(ExternalJoinWritePart, "Number of times a temporary file was written to disk for JOIN in external memory.") \
    M(ExternalJoinMerge, "Number of times temporary files were merged for JOIN in external memory.") \
    M(ExternalJoinCompressedBytes, "Number of compressed bytes written for JOIN in external memory.") \
    M(ExternalJoinUncompressedBytes, "Amount of data (uncompressed, before compression) written for JOIN in external memory.") \
    \
    M(SlowRead, "Number of reads from a file that were slow. This indicate system overload. Thresholds are controlled by read_backoff_* settings.") \
    M(ReadBackoff, "Number of times the number of query processing threads was lowered due to slow reads.") \
    \
    M(ReplicaPartialShutdown, "How many times Replicated table has to deinitialize its state due to session expiration in ZooKeeper. The state is reinitialized every time when ZooKeeper is available again.") \
    \
    M(SelectedParts, "Number of data parts selected to read from a MergeTree table.") \
    M(SelectedPartsTotal, "Number of total data parts before selecting which ones to read from a MergeTree table.") \
    M(SelectedRanges, "Number of (non-adjacent) ranges in all data parts selected to read from a MergeTree table.") \
    M(SelectedMarks, "Number of marks (index granules) selected to read from a MergeTree table.") \
    M(SelectedMarksTotal, "Number of total marks (index granules) before selecting which ones to read from a MergeTree table.") \
    M(SelectedRows, "Number of rows SELECTed from all tables.") \
    M(SelectedBytes, "Number of bytes (uncompressed; for columns as they stored in memory) SELECTed from all tables.") \
    M(RowsReadByMainReader, "Number of rows read from MergeTree tables by the main reader (after PREWHERE step).") \
    M(RowsReadByPrewhereReaders, "Number of rows read from MergeTree tables (in total) by prewhere readers.") \
    \
    M(WaitMarksLoadMicroseconds, "Time spent loading marks") \
    M(BackgroundLoadingMarksTasks, "Number of background tasks for loading marks") \
    M(LoadedMarksCount, "Number of marks loaded (total across columns).") \
    M(LoadedMarksMemoryBytes, "Size of in-memory representations of loaded marks.") \
    \
    M(Merge, "Number of launched background merges.") \
    M(MergedRows, "Rows read for background merges. This is the number of rows before merge.") \
    M(MergedUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) that was read for background merges. This is the number before merge.") \
    M(MergesTimeMilliseconds, "Total time spent for background merges.")\
    \
    M(MergeTreeDataWriterRows, "Number of rows INSERTed to MergeTree tables.") \
    M(MergeTreeDataWriterUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables.") \
    M(MergeTreeDataWriterCompressedBytes, "Bytes written to filesystem for data INSERTed to MergeTree tables.") \
    M(MergeTreeDataWriterBlocks, "Number of blocks INSERTed to MergeTree tables. Each block forms a data part of level zero.") \
    M(MergeTreeDataWriterBlocksAlreadySorted, "Number of blocks INSERTed to MergeTree tables that appeared to be already sorted.") \
    \
    M(MergeTreeDataWriterSkipIndicesCalculationMicroseconds, "Time spent calculating skip indices") \
    M(MergeTreeDataWriterStatisticsCalculationMicroseconds, "Time spent calculating statistics") \
    M(MergeTreeDataWriterSortingBlocksMicroseconds, "Time spent sorting blocks") \
    M(MergeTreeDataWriterMergingBlocksMicroseconds, "Time spent merging input blocks (for special MergeTree engines)") \
    M(MergeTreeDataWriterProjectionsCalculationMicroseconds, "Time spent calculating projections") \
    M(MergeTreeDataProjectionWriterSortingBlocksMicroseconds, "Time spent sorting blocks (for projection it might be a key different from table's sorting key)") \
    M(MergeTreeDataProjectionWriterMergingBlocksMicroseconds, "Time spent merging blocks") \
    M(MutateTaskProjectionsCalculationMicroseconds, "Time spent calculating projections") \
    \
    M(InsertedWideParts, "Number of parts inserted in Wide format.") \
    M(InsertedCompactParts, "Number of parts inserted in Compact format.") \
    M(MergedIntoWideParts, "Number of parts merged into Wide format.") \
    M(MergedIntoCompactParts, "Number of parts merged into Compact format.") \
    \
    M(MergeTreeDataProjectionWriterRows, "Number of rows INSERTed to MergeTree tables projection.") \
    M(MergeTreeDataProjectionWriterUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables projection.") \
    M(MergeTreeDataProjectionWriterCompressedBytes, "Bytes written to filesystem for data INSERTed to MergeTree tables projection.") \
    M(MergeTreeDataProjectionWriterBlocks, "Number of blocks INSERTed to MergeTree tables projection. Each block forms a data part of level zero.") \
    M(MergeTreeDataProjectionWriterBlocksAlreadySorted, "Number of blocks INSERTed to MergeTree tables projection that appeared to be already sorted.") \
    \
    M(CannotRemoveEphemeralNode, "Number of times an error happened while trying to remove ephemeral node. This is not an issue, because our implementation of ZooKeeper library guarantee that the session will expire and the node will be removed.") \
    \
    M(RegexpWithMultipleNeedlesCreated, "Regular expressions with multiple needles (VectorScan library) compiled.") \
    M(RegexpWithMultipleNeedlesGlobalCacheHit, "Number of times we fetched compiled regular expression with multiple needles (VectorScan library) from the global cache.") \
    M(RegexpWithMultipleNeedlesGlobalCacheMiss, "Number of times we failed to fetch compiled regular expression with multiple needles (VectorScan library) from the global cache.") \
    M(RegexpLocalCacheHit, "Number of times we fetched compiled regular expression from a local cache.") \
    M(RegexpLocalCacheMiss, "Number of times we failed to fetch compiled regular expression from a local cache.") \
    \
    M(ContextLock, "Number of times the lock of Context was acquired or tried to acquire. This is global lock.") \
    M(ContextLockWaitMicroseconds, "Context lock wait time in microseconds") \
    \
    M(StorageBufferFlush, "Number of times a buffer in a 'Buffer' table was flushed.") \
    M(StorageBufferErrorOnFlush, "Number of times a buffer in the 'Buffer' table has not been able to flush due to error writing in the destination table.") \
    M(StorageBufferPassedAllMinThresholds, "Number of times a criteria on min thresholds has been reached to flush a buffer in a 'Buffer' table.") \
    M(StorageBufferPassedTimeMaxThreshold, "Number of times a criteria on max time threshold has been reached to flush a buffer in a 'Buffer' table.") \
    M(StorageBufferPassedRowsMaxThreshold, "Number of times a criteria on max rows threshold has been reached to flush a buffer in a 'Buffer' table.") \
    M(StorageBufferPassedBytesMaxThreshold, "Number of times a criteria on max bytes threshold has been reached to flush a buffer in a 'Buffer' table.") \
    M(StorageBufferPassedTimeFlushThreshold, "Number of times background-only flush threshold on time has been reached to flush a buffer in a 'Buffer' table. This is expert-only metric. If you read this and you are not an expert, stop reading.") \
    M(StorageBufferPassedRowsFlushThreshold, "Number of times background-only flush threshold on rows has been reached to flush a buffer in a 'Buffer' table. This is expert-only metric. If you read this and you are not an expert, stop reading.") \
    M(StorageBufferPassedBytesFlushThreshold, "Number of times background-only flush threshold on bytes has been reached to flush a buffer in a 'Buffer' table. This is expert-only metric. If you read this and you are not an expert, stop reading.") \
    M(StorageBufferLayerLockReadersWaitMilliseconds, "Time for waiting for Buffer layer during reading.") \
    M(StorageBufferLayerLockWritersWaitMilliseconds, "Time for waiting free Buffer layer to write to (can be used to tune Buffer layers).") \
    \
    M(DictCacheKeysRequested, "Number of keys requested from the data source for the dictionaries of 'cache' types.") \
    M(DictCacheKeysRequestedMiss, "Number of keys requested from the data source for dictionaries of 'cache' types but not found in the data source.") \
    M(DictCacheKeysRequestedFound, "Number of keys requested from the data source for dictionaries of 'cache' types and found in the data source.") \
    M(DictCacheKeysExpired, "Number of keys looked up in the dictionaries of 'cache' types and found in the cache but they were obsolete.") \
    M(DictCacheKeysNotFound, "Number of keys looked up in the dictionaries of 'cache' types and not found.") \
    M(DictCacheKeysHit, "Number of keys looked up in the dictionaries of 'cache' types and found in the cache.") \
    M(DictCacheRequestTimeNs, "Number of nanoseconds spend in querying the external data sources for the dictionaries of 'cache' types.") \
    M(DictCacheRequests, "Number of bulk requests to the external data sources for the dictionaries of 'cache' types.") \
    M(DictCacheLockWriteNs, "Number of nanoseconds spend in waiting for write lock to update the data for the dictionaries of 'cache' types.") \
    M(DictCacheLockReadNs, "Number of nanoseconds spend in waiting for read lock to lookup the data for the dictionaries of 'cache' types.") \
    \
    M(DistributedSyncInsertionTimeoutExceeded, "A timeout has exceeded while waiting for shards during synchronous insertion into a Distributed table (with 'distributed_foreground_insert' = 1)") \
    M(DistributedAsyncInsertionFailures, "Number of failures for asynchronous insertion into a Distributed table (with 'distributed_foreground_insert' = 0)") \
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
)") \
    M(DataAfterMutationDiffersFromReplica, "Number of times data after mutation is not byte-identical to the data on other replicas. In addition to the reasons described in 'DataAfterMergeDiffersFromReplica', it is also possible due to non-deterministic mutation.") \
    M(PolygonsAddedToPool, "A polygon has been added to the cache (pool) for the 'pointInPolygon' function.") \
    M(PolygonsInPoolAllocatedBytes, "The number of bytes for polygons added to the cache (pool) for the 'pointInPolygon' function.") \
    \
    M(USearchAddCount, "Number of vectors added to usearch indexes.") \
    M(USearchAddVisitedMembers, "Number of nodes visited when adding vectors to usearch indexes.") \
    M(USearchAddComputedDistances, "Number of times distance was computed when adding vectors to usearch indexes.") \
    M(USearchSearchCount, "Number of search operations performed in usearch indexes.") \
    M(USearchSearchVisitedMembers, "Number of nodes visited when searching in usearch indexes.") \
    M(USearchSearchComputedDistances, "Number of times distance was computed when searching usearch indexes.") \
    \
    M(RWLockAcquiredReadLocks, "Number of times a read lock was acquired (in a heavy RWLock).") \
    M(RWLockAcquiredWriteLocks, "Number of times a write lock was acquired (in a heavy RWLock).") \
    M(RWLockReadersWaitMilliseconds, "Total time spent waiting for a read lock to be acquired (in a heavy RWLock).") \
    M(RWLockWritersWaitMilliseconds, "Total time spent waiting for a write lock to be acquired (in a heavy RWLock).") \
    M(DNSError, "Total count of errors in DNS resolution") \
    M(PartsLockHoldMicroseconds, "Total time spent holding data parts lock in MergeTree tables") \
    M(PartsLockWaitMicroseconds, "Total time spent waiting for data parts lock in MergeTree tables") \
    \
    M(RealTimeMicroseconds, "Total (wall clock) time spent in processing (queries and other tasks) threads (note that this is a sum).") \
    M(UserTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in user mode. This includes time CPU pipeline was stalled due to main memory access, cache misses, branch mispredictions, hyper-threading, etc.") \
    M(SystemTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in OS kernel mode. This is time spent in syscalls, excluding waiting time during blocking syscalls.") \
    M(MemoryOvercommitWaitTimeMicroseconds, "Total time spent in waiting for memory to be freed in OvercommitTracker.") \
    M(MemoryAllocatorPurge, "Total number of times memory allocator purge was requested") \
    M(MemoryAllocatorPurgeTimeMicroseconds, "Total number of times memory allocator purge was requested") \
    M(SoftPageFaults, "The number of soft page faults in query execution threads. Soft page fault usually means a miss in the memory allocator cache, which requires a new memory mapping from the OS and subsequent allocation of a page of physical memory.") \
    M(HardPageFaults, "The number of hard page faults in query execution threads. High values indicate either that you forgot to turn off swap on your server, or eviction of memory pages of the ClickHouse binary during very high memory pressure, or successful usage of the 'mmap' read method for the tables data.") \
    \
    M(OSIOWaitMicroseconds, "Total time a thread spent waiting for a result of IO operation, from the OS point of view. This is real IO that doesn't include page cache.") \
    M(OSCPUWaitMicroseconds, "Total time a thread was ready for execution but waiting to be scheduled by OS, from the OS point of view.") \
    M(OSCPUVirtualTimeMicroseconds, "CPU time spent seen by OS. Does not include involuntary waits due to virtualization.") \
    M(OSReadBytes, "Number of bytes read from disks or block devices. Doesn't include bytes read from page cache. May include excessive data due to block size, readahead, etc.") \
    M(OSWriteBytes, "Number of bytes written to disks or block devices. Doesn't include bytes that are in page cache dirty pages. May not include data that was written by OS asynchronously.") \
    M(OSReadChars, "Number of bytes read from filesystem, including page cache.") \
    M(OSWriteChars, "Number of bytes written to filesystem, including page cache.") \
    \
    M(ParallelReplicasHandleRequestMicroseconds, "Time spent processing requests for marks from replicas") \
    M(ParallelReplicasHandleAnnouncementMicroseconds, "Time spent processing replicas announcements") \
    \
    M(ParallelReplicasReadAssignedMarks, "Sum across all replicas of how many of scheduled marks were assigned by consistent hash") \
    M(ParallelReplicasReadUnassignedMarks, "Sum across all replicas of how many unassigned marks were scheduled") \
    M(ParallelReplicasReadAssignedForStealingMarks, "Sum across all replicas of how many of scheduled marks were assigned for stealing by consistent hash") \
    \
    M(ParallelReplicasStealingByHashMicroseconds, "Time spent collecting segments meant for stealing by hash") \
    M(ParallelReplicasProcessingPartsMicroseconds, "Time spent processing data parts") \
    M(ParallelReplicasStealingLeftoversMicroseconds, "Time spent collecting orphaned segments") \
    M(ParallelReplicasCollectingOwnedSegmentsMicroseconds, "Time spent collecting segments meant by hash") \
    M(ParallelReplicasNumRequests, "Number of requests to the initiator.") \
    M(ParallelReplicasDeniedRequests, "Number of completely denied requests to the initiator") \
    M(CacheWarmerBytesDownloaded, "Amount of data fetched into filesystem cache by dedicated background threads.") \
    M(CacheWarmerDataPartsDownloaded, "Number of data parts that were fully fetched by CacheWarmer.") \
    M(IgnoredColdParts, "See setting ignore_cold_parts_seconds. Number of times read queries ignored very new parts that weren't pulled into cache by CacheWarmer yet.") \
    M(PreferredWarmedUnmergedParts, "See setting prefer_warmed_unmerged_parts_seconds. Number of times read queries used outdated pre-merge parts that are in cache instead of merged part that wasn't pulled into cache by CacheWarmer yet.") \
    \
    M(PerfCPUCycles, "Total cycles. Be wary of what happens during CPU frequency scaling.")  \
    M(PerfInstructions, "Retired instructions. Be careful, these can be affected by various issues, most notably hardware interrupt counts.") \
    M(PerfCacheReferences, "Cache accesses. Usually, this indicates Last Level Cache accesses, but this may vary depending on your CPU. This may include prefetches and coherency messages; again this depends on the design of your CPU.") \
    M(PerfCacheMisses, "Cache misses. Usually this indicates Last Level Cache misses; this is intended to be used in conjunction with the PERFCOUNTHWCACHEREFERENCES event to calculate cache miss rates.") \
    M(PerfBranchInstructions, "Retired branch instructions. Prior to Linux 2.6.35, this used the wrong event on AMD processors.") \
    M(PerfBranchMisses, "Mispredicted branch instructions.") \
    M(PerfBusCycles, "Bus cycles, which can be different from total cycles.") \
    M(PerfStalledCyclesFrontend, "Stalled cycles during issue.") \
    M(PerfStalledCyclesBackend, "Stalled cycles during retirement.") \
    M(PerfRefCPUCycles, "Total cycles; not affected by CPU frequency scaling.") \
    \
    M(PerfCPUClock, "The CPU clock, a high-resolution per-CPU timer") \
    M(PerfTaskClock, "A clock count specific to the task that is running") \
    M(PerfContextSwitches, "Number of context switches") \
    M(PerfCPUMigrations, "Number of times the process has migrated to a new CPU") \
    M(PerfAlignmentFaults, "Number of alignment faults. These happen when unaligned memory accesses happen; the kernel can handle these but it reduces performance. This happens only on some architectures (never on x86).") \
    M(PerfEmulationFaults, "Number of emulation faults. The kernel sometimes traps on unimplemented instructions and emulates them for user space. This can negatively impact performance.") \
    M(PerfMinEnabledTime, "For all events, minimum time that an event was enabled. Used to track event multiplexing influence") \
    M(PerfMinEnabledRunningTime, "Running time for event with minimum enabled time. Used to track the amount of event multiplexing") \
    M(PerfDataTLBReferences, "Data TLB references") \
    M(PerfDataTLBMisses, "Data TLB misses") \
    M(PerfInstructionTLBReferences, "Instruction TLB references") \
    M(PerfInstructionTLBMisses, "Instruction TLB misses") \
    M(PerfLocalMemoryReferences, "Local NUMA node memory reads") \
    M(PerfLocalMemoryMisses, "Local NUMA node memory read misses") \
    \
    M(CannotWriteToWriteBufferDiscard, "Number of stack traces dropped by query profiler or signal handler because pipe is full or cannot write to pipe.") \
    M(QueryProfilerSignalOverruns, "Number of times we drop processing of a query profiler signal due to overrun plus the number of signals that OS has not delivered due to overrun.") \
    M(QueryProfilerConcurrencyOverruns, "Number of times we drop processing of a query profiler signal due to too many concurrent query profilers in other threads, which may indicate overload.") \
    M(QueryProfilerRuns, "Number of times QueryProfiler had been run.") \
    M(QueryProfilerErrors, "Invalid memory accesses during asynchronous stack unwinding.") \
    \
    M(CreatedLogEntryForMerge, "Successfully created log entry to merge parts in ReplicatedMergeTree.") \
    M(NotCreatedLogEntryForMerge, "Log entry to merge parts in ReplicatedMergeTree is not created due to concurrent log update by another replica.") \
    M(CreatedLogEntryForMutation, "Successfully created log entry to mutate parts in ReplicatedMergeTree.") \
    M(NotCreatedLogEntryForMutation, "Log entry to mutate parts in ReplicatedMergeTree is not created due to concurrent log update by another replica.") \
    \
    M(S3ReadMicroseconds, "Time of GET and HEAD requests to S3 storage.") \
    M(S3ReadRequestsCount, "Number of GET and HEAD requests to S3 storage.") \
    M(S3ReadRequestsErrors, "Number of non-throttling errors in GET and HEAD requests to S3 storage.") \
    M(S3ReadRequestsThrottling, "Number of 429 and 503 errors in GET and HEAD requests to S3 storage.") \
    M(S3ReadRequestsRedirects, "Number of redirects in GET and HEAD requests to S3 storage.") \
    \
    M(S3WriteMicroseconds, "Time of POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteRequestsCount, "Number of POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteRequestsErrors, "Number of non-throttling errors in POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteRequestsThrottling, "Number of 429 and 503 errors in POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteRequestsRedirects, "Number of redirects in POST, DELETE, PUT and PATCH requests to S3 storage.") \
    \
    M(DiskS3ReadMicroseconds, "Time of GET and HEAD requests to DiskS3 storage.") \
    M(DiskS3ReadRequestsCount, "Number of GET and HEAD requests to DiskS3 storage.") \
    M(DiskS3ReadRequestsErrors, "Number of non-throttling errors in GET and HEAD requests to DiskS3 storage.") \
    M(DiskS3ReadRequestsThrottling, "Number of 429 and 503 errors in GET and HEAD requests to DiskS3 storage.") \
    M(DiskS3ReadRequestsRedirects, "Number of redirects in GET and HEAD requests to DiskS3 storage.") \
    \
    M(DiskS3WriteMicroseconds, "Time of POST, DELETE, PUT and PATCH requests to DiskS3 storage.") \
    M(DiskS3WriteRequestsCount, "Number of POST, DELETE, PUT and PATCH requests to DiskS3 storage.") \
    M(DiskS3WriteRequestsErrors, "Number of non-throttling errors in POST, DELETE, PUT and PATCH requests to DiskS3 storage.") \
    M(DiskS3WriteRequestsThrottling, "Number of 429 and 503 errors in POST, DELETE, PUT and PATCH requests to DiskS3 storage.") \
    M(DiskS3WriteRequestsRedirects, "Number of redirects in POST, DELETE, PUT and PATCH requests to DiskS3 storage.") \
    \
    M(S3DeleteObjects, "Number of S3 API DeleteObject(s) calls.") \
    M(S3CopyObject, "Number of S3 API CopyObject calls.") \
    M(S3ListObjects, "Number of S3 API ListObjects calls.") \
    M(S3HeadObject,  "Number of S3 API HeadObject calls.") \
    M(S3GetObjectAttributes, "Number of S3 API GetObjectAttributes calls.") \
    M(S3CreateMultipartUpload, "Number of S3 API CreateMultipartUpload calls.") \
    M(S3UploadPartCopy, "Number of S3 API UploadPartCopy calls.") \
    M(S3UploadPart, "Number of S3 API UploadPart calls.") \
    M(S3AbortMultipartUpload, "Number of S3 API AbortMultipartUpload calls.") \
    M(S3CompleteMultipartUpload, "Number of S3 API CompleteMultipartUpload calls.") \
    M(S3PutObject, "Number of S3 API PutObject calls.") \
    M(S3GetObject, "Number of S3 API GetObject calls.") \
    \
    M(DiskS3DeleteObjects, "Number of DiskS3 API DeleteObject(s) calls.") \
    M(DiskS3CopyObject, "Number of DiskS3 API CopyObject calls.") \
    M(DiskS3ListObjects, "Number of DiskS3 API ListObjects calls.") \
    M(DiskS3HeadObject,  "Number of DiskS3 API HeadObject calls.") \
    M(DiskS3GetObjectAttributes, "Number of DiskS3 API GetObjectAttributes calls.") \
    M(DiskS3CreateMultipartUpload, "Number of DiskS3 API CreateMultipartUpload calls.") \
    M(DiskS3UploadPartCopy, "Number of DiskS3 API UploadPartCopy calls.") \
    M(DiskS3UploadPart, "Number of DiskS3 API UploadPart calls.") \
    M(DiskS3AbortMultipartUpload, "Number of DiskS3 API AbortMultipartUpload calls.") \
    M(DiskS3CompleteMultipartUpload, "Number of DiskS3 API CompleteMultipartUpload calls.") \
    M(DiskS3PutObject, "Number of DiskS3 API PutObject calls.") \
    M(DiskS3GetObject, "Number of DiskS3 API GetObject calls.") \
    \
    M(DiskPlainRewritableAzureDirectoryCreated, "Number of directories created by the 'plain_rewritable' metadata storage for AzureObjectStorage.") \
    M(DiskPlainRewritableAzureDirectoryRemoved, "Number of directories removed by the 'plain_rewritable' metadata storage for AzureObjectStorage.") \
    M(DiskPlainRewritableLocalDirectoryCreated, "Number of directories created by the 'plain_rewritable' metadata storage for LocalObjectStorage.") \
    M(DiskPlainRewritableLocalDirectoryRemoved, "Number of directories removed by the 'plain_rewritable' metadata storage for LocalObjectStorage.") \
    M(DiskPlainRewritableS3DirectoryCreated, "Number of directories created by the 'plain_rewritable' metadata storage for S3ObjectStorage.") \
    M(DiskPlainRewritableS3DirectoryRemoved, "Number of directories removed by the 'plain_rewritable' metadata storage for S3ObjectStorage.") \
    \
    M(S3Clients, "Number of created S3 clients.") \
    M(TinyS3Clients, "Number of S3 clients copies which reuse an existing auth provider from another client.") \
    \
    M(EngineFileLikeReadFiles, "Number of files read in table engines working with files (like File/S3/URL/HDFS).") \
    \
    M(ReadBufferFromS3Microseconds, "Time spent on reading from S3.") \
    M(ReadBufferFromS3InitMicroseconds, "Time spent initializing connection to S3.") \
    M(ReadBufferFromS3Bytes, "Bytes read from S3.") \
    M(ReadBufferFromS3RequestsErrors, "Number of exceptions while reading from S3.") \
    \
    M(WriteBufferFromS3Microseconds, "Time spent on writing to S3.") \
    M(WriteBufferFromS3Bytes, "Bytes written to S3.") \
    M(WriteBufferFromS3RequestsErrors, "Number of exceptions while writing to S3.") \
    M(WriteBufferFromS3WaitInflightLimitMicroseconds, "Time spent on waiting while some of the current requests are done when its number reached the limit defined by s3_max_inflight_parts_for_one_file.") \
    M(QueryMemoryLimitExceeded, "Number of times when memory limit exceeded for query.") \
    \
    M(AzureGetObject, "Number of Azure API GetObject calls.") \
    M(AzureUpload, "Number of Azure blob storage API Upload calls") \
    M(AzureStageBlock, "Number of Azure blob storage API StageBlock calls") \
    M(AzureCommitBlockList, "Number of Azure blob storage API CommitBlockList calls") \
    M(AzureCopyObject, "Number of Azure blob storage API CopyObject calls") \
    M(AzureDeleteObjects, "Number of Azure blob storage API DeleteObject(s) calls.") \
    M(AzureListObjects, "Number of Azure blob storage API ListObjects calls.") \
    M(AzureGetProperties, "Number of Azure blob storage API GetProperties calls.") \
    M(AzureCreateContainer, "Number of Azure blob storage API CreateContainer calls.") \
    \
    M(DiskAzureGetObject, "Number of Disk Azure API GetObject calls.") \
    M(DiskAzureUpload, "Number of Disk Azure blob storage API Upload calls") \
    M(DiskAzureStageBlock, "Number of Disk Azure blob storage API StageBlock calls") \
    M(DiskAzureCommitBlockList, "Number of Disk Azure blob storage API CommitBlockList calls") \
    M(DiskAzureCopyObject, "Number of Disk Azure blob storage API CopyObject calls") \
    M(DiskAzureListObjects, "Number of Disk Azure blob storage API ListObjects calls.") \
    M(DiskAzureDeleteObjects, "Number of Disk Azure blob storage API DeleteObject(s) calls.") \
    M(DiskAzureGetProperties, "Number of Disk Azure blob storage API GetProperties calls.") \
    M(DiskAzureCreateContainer, "Number of Disk Azure blob storage API CreateContainer calls.") \
    \
    M(ReadBufferFromAzureMicroseconds, "Time spent on reading from Azure.") \
    M(ReadBufferFromAzureInitMicroseconds, "Time spent initializing connection to Azure.") \
    M(ReadBufferFromAzureBytes, "Bytes read from Azure.") \
    M(ReadBufferFromAzureRequestsErrors, "Number of exceptions while reading from Azure") \
    \
    M(CachedReadBufferReadFromCacheHits, "Number of times the read from filesystem cache hit the cache.") \
    M(CachedReadBufferReadFromCacheMisses, "Number of times the read from filesystem cache miss the cache.") \
    M(CachedReadBufferReadFromSourceMicroseconds, "Time reading from filesystem cache source (from remote filesystem, etc)") \
    M(CachedReadBufferReadFromCacheMicroseconds, "Time reading from filesystem cache") \
    M(CachedReadBufferReadFromSourceBytes, "Bytes read from filesystem cache source (from remote fs, etc)") \
    M(CachedReadBufferReadFromCacheBytes, "Bytes read from filesystem cache") \
    M(CachedReadBufferCacheWriteBytes, "Bytes written from source (remote fs, etc) to filesystem cache") \
    M(CachedReadBufferCacheWriteMicroseconds, "Time spent writing data into filesystem cache") \
    M(CachedReadBufferCreateBufferMicroseconds, "Prepare buffer time") \
    M(CachedWriteBufferCacheWriteBytes, "Bytes written from source (remote fs, etc) to filesystem cache") \
    M(CachedWriteBufferCacheWriteMicroseconds, "Time spent writing data into filesystem cache") \
    \
    M(FilesystemCacheLoadMetadataMicroseconds, "Time spent loading filesystem cache metadata") \
    M(FilesystemCacheEvictedBytes, "Number of bytes evicted from filesystem cache") \
    M(FilesystemCacheEvictedFileSegments, "Number of file segments evicted from filesystem cache") \
    M(FilesystemCacheEvictionSkippedFileSegments, "Number of file segments skipped for eviction because of being in unreleasable state") \
    M(FilesystemCacheEvictionSkippedEvictingFileSegments, "Number of file segments skipped for eviction because of being in evicting state") \
    M(FilesystemCacheEvictionTries, "Number of filesystem cache eviction attempts") \
    M(FilesystemCacheLockKeyMicroseconds, "Lock cache key time") \
    M(FilesystemCacheLockMetadataMicroseconds, "Lock filesystem cache metadata time") \
    M(FilesystemCacheLockCacheMicroseconds, "Lock filesystem cache time") \
    M(FilesystemCacheReserveMicroseconds, "Filesystem cache space reservation time") \
    M(FilesystemCacheEvictMicroseconds, "Filesystem cache eviction time") \
    M(FilesystemCacheGetOrSetMicroseconds, "Filesystem cache getOrSet() time") \
    M(FilesystemCacheGetMicroseconds, "Filesystem cache get() time") \
    M(FileSegmentWaitMicroseconds, "Wait on DOWNLOADING state") \
    M(FileSegmentCompleteMicroseconds, "Duration of FileSegment::complete() in filesystem cache") \
    M(FileSegmentLockMicroseconds, "Lock file segment time") \
    M(FileSegmentWriteMicroseconds, "File segment write() time") \
    M(FileSegmentUseMicroseconds, "File segment use() time") \
    M(FileSegmentRemoveMicroseconds, "File segment remove() time") \
    M(FileSegmentHolderCompleteMicroseconds, "File segments holder complete() time") \
    M(FileSegmentFailToIncreasePriority, "Number of times the priority was not increased due to a high contention on the cache lock") \
    M(FilesystemCacheFailToReserveSpaceBecauseOfLockContention, "Number of times space reservation was skipped due to a high contention on the cache lock") \
    M(FilesystemCacheFailToReserveSpaceBecauseOfCacheResize, "Number of times space reservation was skipped due to the cache is being resized") \
    M(FilesystemCacheHoldFileSegments, "Filesystem cache file segments count, which were hold") \
    M(FilesystemCacheUnusedHoldFileSegments, "Filesystem cache file segments count, which were hold, but not used (because of seek or LIMIT n, etc)") \
    M(FilesystemCacheFreeSpaceKeepingThreadRun, "Number of times background thread executed free space keeping job") \
    M(FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds, "Time for which background thread executed free space keeping job") \
    \
    M(RemoteFSSeeks, "Total number of seeks for async buffer") \
    M(RemoteFSPrefetches, "Number of prefetches made with asynchronous reading from remote filesystem") \
    M(RemoteFSCancelledPrefetches, "Number of cancelled prefecthes (because of seek)") \
    M(RemoteFSUnusedPrefetches, "Number of prefetches pending at buffer destruction") \
    M(RemoteFSPrefetchedReads, "Number of reads from prefecthed buffer") \
    M(RemoteFSPrefetchedBytes, "Number of bytes from prefecthed buffer") \
    M(RemoteFSUnprefetchedReads, "Number of reads from unprefetched buffer") \
    M(RemoteFSUnprefetchedBytes, "Number of bytes from unprefetched buffer") \
    M(RemoteFSLazySeeks, "Number of lazy seeks") \
    M(RemoteFSSeeksWithReset, "Number of seeks which lead to a new connection") \
    M(RemoteFSBuffers, "Number of buffers created for asynchronous reading from remote filesystem") \
    M(MergeTreePrefetchedReadPoolInit, "Time spent preparing tasks in MergeTreePrefetchedReadPool") \
    M(WaitPrefetchTaskMicroseconds, "Time spend waiting for prefetched reader") \
    \
    M(ThreadpoolReaderTaskMicroseconds, "Time spent getting the data in asynchronous reading") \
    M(ThreadpoolReaderPrepareMicroseconds, "Time spent on preparation (e.g. call to reader seek() method)") \
    M(ThreadpoolReaderReadBytes, "Bytes read from a threadpool task in asynchronous reading") \
    M(ThreadpoolReaderSubmit, "Bytes read from a threadpool task in asynchronous reading") \
    M(ThreadpoolReaderSubmitReadSynchronously, "How many times we haven't scheduled a task on the thread pool and read synchronously instead") \
    M(ThreadpoolReaderSubmitReadSynchronouslyBytes, "How many bytes were read synchronously") \
    M(ThreadpoolReaderSubmitReadSynchronouslyMicroseconds, "How much time we spent reading synchronously") \
    M(ThreadpoolReaderSubmitLookupInCacheMicroseconds, "How much time we spent checking if content is cached") \
    M(AsynchronousReaderIgnoredBytes, "Number of bytes ignored during asynchronous reading") \
    \
    M(FileSegmentWaitReadBufferMicroseconds, "Metric per file segment. Time spend waiting for internal read buffer (includes cache waiting)") \
    M(FileSegmentReadMicroseconds, "Metric per file segment. Time spend reading from file") \
    M(FileSegmentCacheWriteMicroseconds, "Metric per file segment. Time spend writing data to cache") \
    M(FileSegmentPredownloadMicroseconds, "Metric per file segment. Time spent pre-downloading data to cache (pre-downloading - finishing file segment download (after someone who failed to do that) up to the point current thread was requested to do)") \
    M(FileSegmentUsedBytes, "Metric per file segment. How many bytes were actually used from current file segment") \
    \
    M(ReadBufferSeekCancelConnection, "Number of seeks which lead to new connection (s3, http)") \
    \
    M(SleepFunctionCalls, "Number of times a sleep function (sleep, sleepEachRow) has been called.") \
    M(SleepFunctionMicroseconds, "Time set to sleep in a sleep function (sleep, sleepEachRow).") \
    M(SleepFunctionElapsedMicroseconds, "Time spent sleeping in a sleep function (sleep, sleepEachRow).") \
    \
    M(ThreadPoolReaderPageCacheHit, "Number of times the read inside ThreadPoolReader was done from the page cache.") \
    M(ThreadPoolReaderPageCacheHitBytes, "Number of bytes read inside ThreadPoolReader when it was done from the page cache.") \
    M(ThreadPoolReaderPageCacheHitElapsedMicroseconds, "Time spent reading data from page cache in ThreadPoolReader.") \
    M(ThreadPoolReaderPageCacheMiss, "Number of times the read inside ThreadPoolReader was not done from page cache and was hand off to thread pool.") \
    M(ThreadPoolReaderPageCacheMissBytes, "Number of bytes read inside ThreadPoolReader when read was not done from page cache and was hand off to thread pool.") \
    M(ThreadPoolReaderPageCacheMissElapsedMicroseconds, "Time spent reading data inside the asynchronous job in ThreadPoolReader - when read was not done from the page cache.") \
    \
    M(AsynchronousReadWaitMicroseconds, "Time spent in waiting for asynchronous reads in asynchronous local read.") \
    M(SynchronousReadWaitMicroseconds, "Time spent in waiting for synchronous reads in asynchronous local read.") \
    M(AsynchronousRemoteReadWaitMicroseconds, "Time spent in waiting for asynchronous remote reads.") \
    M(SynchronousRemoteReadWaitMicroseconds, "Time spent in waiting for synchronous remote reads.") \
    \
    M(ExternalDataSourceLocalCacheReadBytes, "Bytes read from local cache buffer in RemoteReadBufferCache")\
    \
    M(MainConfigLoads, "Number of times the main configuration was reloaded.") \
    \
    M(AggregationPreallocatedElementsInHashTables, "How many elements were preallocated in hash tables for aggregation.") \
    M(AggregationHashTablesInitializedAsTwoLevel, "How many hash tables were inited as two-level for aggregation.") \
    M(AggregationOptimizedEqualRangesOfKeys, "For how many blocks optimization of equal ranges of keys was applied") \
    M(HashJoinPreallocatedElementsInHashTables, "How many elements were preallocated in hash tables for hash join.") \
    \
    M(MetadataFromKeeperCacheHit, "Number of times an object storage metadata request was answered from cache without making request to Keeper") \
    M(MetadataFromKeeperCacheMiss, "Number of times an object storage metadata request had to be answered from Keeper") \
    M(MetadataFromKeeperCacheUpdateMicroseconds, "Total time spent in updating the cache including waiting for responses from Keeper") \
    M(MetadataFromKeeperUpdateCacheOneLevel, "Number of times a cache update for one level of directory tree was done") \
    M(MetadataFromKeeperTransactionCommit, "Number of times metadata transaction commit was attempted") \
    M(MetadataFromKeeperTransactionCommitRetry, "Number of times metadata transaction commit was retried") \
    M(MetadataFromKeeperCleanupTransactionCommit, "Number of times metadata transaction commit for deleted objects cleanup was attempted") \
    M(MetadataFromKeeperCleanupTransactionCommitRetry, "Number of times metadata transaction commit for deleted objects cleanup was retried") \
    M(MetadataFromKeeperOperations, "Number of times a request was made to Keeper") \
    M(MetadataFromKeeperIndividualOperations, "Number of paths read or written by single or multi requests to Keeper") \
    M(MetadataFromKeeperReconnects, "Number of times a reconnect to Keeper was done") \
    M(MetadataFromKeeperBackgroundCleanupObjects, "Number of times a old deleted object clean up was performed by background task") \
    M(MetadataFromKeeperBackgroundCleanupTransactions, "Number of times old transaction idempotency token was cleaned up by background task") \
    M(MetadataFromKeeperBackgroundCleanupErrors, "Number of times an error was encountered in background cleanup task") \
    \
    M(KafkaRebalanceRevocations, "Number of partition revocations (the first stage of consumer group rebalance)") \
    M(KafkaRebalanceAssignments, "Number of partition assignments (the final stage of consumer group rebalance)") \
    M(KafkaRebalanceErrors, "Number of failed consumer group rebalances") \
    M(KafkaMessagesPolled, "Number of Kafka messages polled from librdkafka to ClickHouse") \
    M(KafkaMessagesRead, "Number of Kafka messages already processed by ClickHouse") \
    M(KafkaMessagesFailed, "Number of Kafka messages ClickHouse failed to parse") \
    M(KafkaRowsRead, "Number of rows parsed from Kafka messages") \
    M(KafkaRowsRejected, "Number of parsed rows which were later rejected (due to rebalances / errors or similar reasons). Those rows will be consumed again after the rebalance.") \
    M(KafkaDirectReads, "Number of direct selects from Kafka tables since server start") \
    M(KafkaBackgroundReads, "Number of background reads populating materialized views from Kafka since server start") \
    M(KafkaCommits, "Number of successful commits of consumed offsets to Kafka (normally should be the same as KafkaBackgroundReads)") \
    M(KafkaCommitFailures, "Number of failed commits of consumed offsets to Kafka (usually is a sign of some data duplication)") \
    M(KafkaConsumerErrors, "Number of errors reported by librdkafka during polls") \
    M(KafkaWrites, "Number of writes (inserts) to Kafka tables ") \
    M(KafkaRowsWritten, "Number of rows inserted into Kafka tables") \
    M(KafkaProducerFlushes, "Number of explicit flushes to Kafka producer") \
    M(KafkaMessagesProduced, "Number of messages produced to Kafka") \
    M(KafkaProducerErrors, "Number of errors during producing the messages to Kafka") \
    \
    M(ScalarSubqueriesGlobalCacheHit, "Number of times a read from a scalar subquery was done using the global cache") \
    M(ScalarSubqueriesLocalCacheHit, "Number of times a read from a scalar subquery was done using the local cache") \
    M(ScalarSubqueriesCacheMiss, "Number of times a read from a scalar subquery was not cached and had to be calculated completely")                                                                                                                                                                                                 \
    \
    M(SchemaInferenceCacheHits, "Number of times the requested source is found in schema cache") \
    M(SchemaInferenceCacheSchemaHits, "Number of times the schema is found in schema cache during schema inference") \
    M(SchemaInferenceCacheNumRowsHits, "Number of times the number of rows is found in schema cache during count from files") \
    M(SchemaInferenceCacheMisses, "Number of times the requested source is not in schema cache") \
    M(SchemaInferenceCacheSchemaMisses, "Number of times the requested source is in cache but the schema is not in cache during schema inference") \
    M(SchemaInferenceCacheNumRowsMisses, "Number of times the requested source is in cache but the number of rows is not in cache while count from files") \
    M(SchemaInferenceCacheEvictions, "Number of times a schema from cache was evicted due to overflow") \
    M(SchemaInferenceCacheInvalidations, "Number of times a schema in cache became invalid due to changes in data") \
    \
    M(KeeperPacketsSent, "Packets sent by keeper server") \
    M(KeeperPacketsReceived, "Packets received by keeper server") \
    M(KeeperRequestTotal, "Total requests number on keeper server") \
    M(KeeperLatency, "Keeper latency") \
    M(KeeperTotalElapsedMicroseconds, "Keeper total latency for a single request") \
    M(KeeperProcessElapsedMicroseconds, "Keeper commit latency for a single request") \
    M(KeeperPreprocessElapsedMicroseconds, "Keeper preprocessing latency for a single reuquest") \
    M(KeeperStorageLockWaitMicroseconds, "Time spent waiting for acquiring Keeper storage lock") \
    M(KeeperCommitWaitElapsedMicroseconds, "Time spent waiting for certain log to be committed") \
    M(KeeperBatchMaxCount, "Number of times the size of batch was limited by the amount") \
    M(KeeperBatchMaxTotalSize, "Number of times the size of batch was limited by the total bytes size") \
    M(KeeperCommits, "Number of successful commits") \
    M(KeeperCommitsFailed, "Number of failed commits") \
    M(KeeperSnapshotCreations, "Number of snapshots creations")\
    M(KeeperSnapshotCreationsFailed, "Number of failed snapshot creations")\
    M(KeeperSnapshotApplys, "Number of snapshot applying")\
    M(KeeperSnapshotApplysFailed, "Number of failed snapshot applying")\
    M(KeeperReadSnapshot, "Number of snapshot read(serialization)")\
    M(KeeperSaveSnapshot, "Number of snapshot save")\
    M(KeeperCreateRequest, "Number of create requests")\
    M(KeeperRemoveRequest, "Number of remove requests")\
    M(KeeperSetRequest, "Number of set requests")\
    M(KeeperReconfigRequest, "Number of reconfig requests")\
    M(KeeperCheckRequest, "Number of check requests")\
    M(KeeperMultiRequest, "Number of multi requests")\
    M(KeeperMultiReadRequest, "Number of multi read requests")\
    M(KeeperGetRequest, "Number of get requests")\
    M(KeeperListRequest, "Number of list requests")\
    M(KeeperExistsRequest, "Number of exists requests")\
    \
    M(OverflowBreak, "Number of times, data processing was cancelled by query complexity limitation with setting '*_overflow_mode' = 'break' and the result is incomplete.") \
    M(OverflowThrow, "Number of times, data processing was cancelled by query complexity limitation with setting '*_overflow_mode' = 'throw' and exception was thrown.") \
    M(OverflowAny, "Number of times approximate GROUP BY was in effect: when aggregation was performed only on top of first 'max_rows_to_group_by' unique keys and other keys were ignored due to 'group_by_overflow_mode' = 'any'.") \
    \
    M(S3QueueSetFileProcessingMicroseconds, "Time spent to set file as processing")\
    M(S3QueueSetFileProcessedMicroseconds, "Time spent to set file as processed")\
    M(S3QueueSetFileFailedMicroseconds, "Time spent to set file as failed")\
    M(ObjectStorageQueueFailedFiles, "Number of files which failed to be processed")\
    M(ObjectStorageQueueProcessedFiles, "Number of files which were processed")\
    M(ObjectStorageQueueCleanupMaxSetSizeOrTTLMicroseconds, "Time spent to set file as failed")\
    M(ObjectStorageQueuePullMicroseconds, "Time spent to read file data")\
    M(ObjectStorageQueueLockLocalFileStatusesMicroseconds, "Time spent to lock local file statuses")\
    \
    M(ServerStartupMilliseconds, "Time elapsed from starting server to listening to sockets in milliseconds")\
    M(IOUringSQEsSubmitted, "Total number of io_uring SQEs submitted") \
    M(IOUringSQEsResubmitsAsync, "Total number of asynchronous io_uring SQE resubmits performed") \
    M(IOUringSQEsResubmitsSync, "Total number of synchronous io_uring SQE resubmits performed") \
    M(IOUringCQEsCompleted, "Total number of successfully completed io_uring CQEs") \
    M(IOUringCQEsFailed, "Total number of completed io_uring CQEs with failures") \
    \
    M(BackupsOpenedForRead, "Number of backups opened for reading") \
    M(BackupsOpenedForWrite, "Number of backups opened for writing") \
    M(BackupReadMetadataMicroseconds, "Time spent reading backup metadata from .backup file") \
    M(BackupWriteMetadataMicroseconds, "Time spent writing backup metadata to .backup file") \
    M(BackupEntriesCollectorMicroseconds, "Time spent making backup entries") \
    M(BackupEntriesCollectorForTablesDataMicroseconds, "Time spent making backup entries for tables data") \
    M(BackupEntriesCollectorRunPostTasksMicroseconds, "Time spent running post tasks after making backup entries") \
    \
    M(ReadTaskRequestsReceived, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the initiator server side.") \
    M(MergeTreeReadTaskRequestsReceived, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the initiator server side.") \
    \
    M(ReadTaskRequestsSent, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the remote server side.") \
    M(MergeTreeReadTaskRequestsSent, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the remote server side.") \
    M(MergeTreeAllRangesAnnouncementsSent, "The number of announcements sent from the remote server to the initiator server about the set of data parts (for MergeTree tables). Measured on the remote server side.") \
    M(ReadTaskRequestsSentElapsedMicroseconds, "Time spent in callbacks requested from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the remote server side.") \
    M(MergeTreeReadTaskRequestsSentElapsedMicroseconds, "Time spent in callbacks requested from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the remote server side.") \
    M(MergeTreeAllRangesAnnouncementsSentElapsedMicroseconds, "Time spent in sending the announcement from the remote server to the initiator server about the set of data parts (for MergeTree tables). Measured on the remote server side.") \
    \
    M(ConnectionPoolIsFullMicroseconds, "Total time spent waiting for a slot in connection pool.") \
    M(AsyncLoaderWaitMicroseconds, "Total time a query was waiting for async loader jobs.") \
    \
    M(DistrCacheServerSwitches, "Number of server switches between distributed cache servers in read/write-through cache") \
    M(DistrCacheReadMicroseconds, "Time spent reading from distributed cache") \
    M(DistrCacheFallbackReadMicroseconds, "Time spend reading from fallback buffer instead of distribted cache") \
    M(DistrCachePrecomputeRangesMicroseconds, "Time spent to precompute read ranges") \
    M(DistrCacheNextImplMicroseconds, "Time spend in ReadBufferFromDistributedCache::nextImpl") \
    M(DistrCacheOpenedConnections, "The number of open connections to distributed cache") \
    M(DistrCacheReusedConnections, "The number of reused connections to distributed cache") \
    M(DistrCacheHoldConnections, "The number of used connections to distributed cache") \
    \
    M(DistrCacheGetResponseMicroseconds, "Time spend to wait for response from distributed cache") \
    M(DistrCacheStartRangeMicroseconds, "Time spent to start a new read range with distributed cache") \
    M(DistrCacheLockRegistryMicroseconds, "Time spent to take DistributedCacheRegistry lock") \
    M(DistrCacheUnusedPackets, "Number of skipped unused packets from distributed cache") \
    M(DistrCachePackets, "Total number of packets received from distributed cache") \
    M(DistrCacheUnusedPacketsBytes, "The number of bytes in Data packets which were ignored") \
    M(DistrCacheRegistryUpdateMicroseconds, "Time spent updating distributed cache registry") \
    M(DistrCacheRegistryUpdates, "Number of distributed cache registry updates") \
    \
    M(DistrCacheConnectMicroseconds, "The time spent to connect to distributed cache") \
    M(DistrCacheConnectAttempts, "The number of connection attempts to distributed cache") \
    M(DistrCacheGetClient, "Number of client access times") \
    \
    M(DistrCacheServerProcessRequestMicroseconds, "Time spent processing request on DistributedCache server side") \
    \
    M(LogTest, "Number of log messages with level Test") \
    M(LogTrace, "Number of log messages with level Trace") \
    M(LogDebug, "Number of log messages with level Debug") \
    M(LogInfo, "Number of log messages with level Info") \
    M(LogWarning, "Number of log messages with level Warning") \
    M(LogError, "Number of log messages with level Error") \
    M(LogFatal, "Number of log messages with level Fatal") \
    \
    M(InterfaceHTTPSendBytes, "Number of bytes sent through HTTP interfaces") \
    M(InterfaceHTTPReceiveBytes, "Number of bytes received through HTTP interfaces") \
    M(InterfaceNativeSendBytes, "Number of bytes sent through native interfaces") \
    M(InterfaceNativeReceiveBytes, "Number of bytes received through native interfaces") \
    M(InterfacePrometheusSendBytes, "Number of bytes sent through Prometheus interfaces") \
    M(InterfacePrometheusReceiveBytes, "Number of bytes received through Prometheus interfaces") \
    M(InterfaceInterserverSendBytes, "Number of bytes sent through interserver interfaces") \
    M(InterfaceInterserverReceiveBytes, "Number of bytes received through interserver interfaces") \
    M(InterfaceMySQLSendBytes, "Number of bytes sent through MySQL interfaces") \
    M(InterfaceMySQLReceiveBytes, "Number of bytes received through MySQL interfaces") \
    M(InterfacePostgreSQLSendBytes, "Number of bytes sent through PostgreSQL interfaces") \
    M(InterfacePostgreSQLReceiveBytes, "Number of bytes received through PostgreSQL interfaces") \
    \
    M(ParallelReplicasUsedCount, "Number of replicas used to execute a query with task-based parallel replicas") \
    \
    M(KeeperLogsEntryReadFromLatestCache, "Number of log entries in Keeper being read from latest logs cache") \
    M(KeeperLogsEntryReadFromCommitCache, "Number of log entries in Keeper being read from commit logs cache") \
    M(KeeperLogsEntryReadFromFile, "Number of log entries in Keeper being read directly from the changelog file") \
    M(KeeperLogsPrefetchedEntries, "Number of log entries in Keeper being prefetched from the changelog file") \
    \
    M(ParallelReplicasAvailableCount, "Number of replicas available to execute a query with task-based parallel replicas") \
    M(ParallelReplicasUnavailableCount, "Number of replicas which was chosen, but found to be unavailable during query execution with task-based parallel replicas") \
    \
    M(StorageConnectionsCreated, "Number of created connections for storages") \
    M(StorageConnectionsReused, "Number of reused connections for storages") \
    M(StorageConnectionsReset, "Number of reset connections for storages") \
    M(StorageConnectionsPreserved, "Number of preserved connections for storages") \
    M(StorageConnectionsExpired, "Number of expired connections for storages") \
    M(StorageConnectionsErrors, "Number of cases when creation of a connection for storage is failed") \
    M(StorageConnectionsElapsedMicroseconds, "Total time spend on creating connections for storages")                                                                                                                                                                                                                                               \
    \
    M(DiskConnectionsCreated, "Number of created connections for disk") \
    M(DiskConnectionsReused, "Number of reused connections for disk") \
    M(DiskConnectionsReset, "Number of reset connections for disk") \
    M(DiskConnectionsPreserved, "Number of preserved connections for disk") \
    M(DiskConnectionsExpired, "Number of expired connections for disk") \
    M(DiskConnectionsErrors, "Number of cases when creation of a connection for disk is failed") \
    M(DiskConnectionsElapsedMicroseconds, "Total time spend on creating connections for disk") \
    \
    M(HTTPConnectionsCreated, "Number of created http connections") \
    M(HTTPConnectionsReused, "Number of reused http connections") \
    M(HTTPConnectionsReset, "Number of reset http connections") \
    M(HTTPConnectionsPreserved, "Number of preserved http connections") \
    M(HTTPConnectionsExpired, "Number of expired http connections") \
    M(HTTPConnectionsErrors, "Number of cases when creation of a http connection failed") \
    M(HTTPConnectionsElapsedMicroseconds, "Total time spend on creating http connections") \
    \
    M(AddressesDiscovered, "Total count of new addresses in dns resolve results for http connections") \
    M(AddressesExpired, "Total count of expired addresses which is no longer presented in dns resolve results for http connections") \
    M(AddressesMarkedAsFailed, "Total count of addresses which has been marked as faulty due to connection errors for http connections") \
    \
    M(ReadWriteBufferFromHTTPRequestsSent, "Number of HTTP requests sent by ReadWriteBufferFromHTTP") \
    M(ReadWriteBufferFromHTTPBytes, "Total size of payload bytes received and sent by ReadWriteBufferFromHTTP. Doesn't include HTTP headers.") \
    \
    M(GWPAsanAllocateSuccess, "Number of successful allocations done by GWPAsan") \
    M(GWPAsanAllocateFailed, "Number of failed allocations done by GWPAsan (i.e. filled pool)") \
    M(GWPAsanFree, "Number of free operations done by GWPAsan") \


#ifdef APPLY_FOR_EXTERNAL_EVENTS
    #define APPLY_FOR_EVENTS(M) APPLY_FOR_BUILTIN_EVENTS(M) APPLY_FOR_EXTERNAL_EVENTS(M)
#else
    #define APPLY_FOR_EVENTS(M) APPLY_FOR_BUILTIN_EVENTS(M)
#endif

namespace ProfileEvents
{

#define M(NAME, DOCUMENTATION) extern const Event NAME = Event(__COUNTER__);
    APPLY_FOR_EVENTS(M)
#undef M
constexpr Event END = Event(__COUNTER__);

/// Global variable, initialized by zeros.
Counter global_counters_array[END] {};
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
