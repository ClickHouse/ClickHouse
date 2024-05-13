#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <Common/TraceSender.h>


/// Available events. Add something here as you wish.
/// If the event is generic (i.e. not server specific)
/// it should be also added to src/Coordination/KeeperConstant.cpp
#define APPLY_FOR_BUILTIN_EVENTS(M) \
    M(Query, "Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.", ValueType::NUMBER) \
    M(SelectQuery, "Same as Query, but only for SELECT queries.", ValueType::NUMBER) \
    M(InsertQuery, "Same as Query, but only for INSERT queries.", ValueType::NUMBER) \
    M(InitialQuery, "Same as Query, but only counts initial queries (see is_initial_query).", ValueType::NUMBER)\
    M(QueriesWithSubqueries, "Count queries with all subqueries", ValueType::NUMBER) \
    M(SelectQueriesWithSubqueries, "Count SELECT queries with all subqueries", ValueType::NUMBER) \
    M(InsertQueriesWithSubqueries, "Count INSERT queries with all subqueries", ValueType::NUMBER) \
    M(AsyncInsertQuery, "Same as InsertQuery, but only for asynchronous INSERT queries.", ValueType::NUMBER) \
    M(AsyncInsertBytes, "Data size in bytes of asynchronous INSERT queries.", ValueType::BYTES) \
    M(AsyncInsertRows, "Number of rows inserted by asynchronous INSERT queries.", ValueType::NUMBER) \
    M(AsyncInsertCacheHits, "Number of times a duplicate hash id has been found in asynchronous INSERT hash id cache.", ValueType::NUMBER) \
    M(FailedQuery, "Number of failed queries.", ValueType::NUMBER) \
    M(FailedSelectQuery, "Same as FailedQuery, but only for SELECT queries.", ValueType::NUMBER) \
    M(FailedInsertQuery, "Same as FailedQuery, but only for INSERT queries.", ValueType::NUMBER) \
    M(FailedAsyncInsertQuery, "Number of failed ASYNC INSERT queries.", ValueType::NUMBER) \
    M(QueryTimeMicroseconds, "Total time of all queries.", ValueType::MICROSECONDS) \
    M(SelectQueryTimeMicroseconds, "Total time of SELECT queries.", ValueType::MICROSECONDS) \
    M(InsertQueryTimeMicroseconds, "Total time of INSERT queries.", ValueType::MICROSECONDS) \
    M(OtherQueryTimeMicroseconds, "Total time of queries that are not SELECT or INSERT.", ValueType::MICROSECONDS) \
    M(FileOpen, "Number of files opened.", ValueType::NUMBER) \
    M(Seek, "Number of times the 'lseek' function was called.", ValueType::NUMBER) \
    M(ReadBufferFromFileDescriptorRead, "Number of reads (read/pread) from a file descriptor. Does not include sockets.", ValueType::NUMBER) \
    M(ReadBufferFromFileDescriptorReadFailed, "Number of times the read (read/pread) from a file descriptor have failed.", ValueType::NUMBER) \
    M(ReadBufferFromFileDescriptorReadBytes, "Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.", ValueType::BYTES) \
    M(WriteBufferFromFileDescriptorWrite, "Number of writes (write/pwrite) to a file descriptor. Does not include sockets.", ValueType::NUMBER) \
    M(WriteBufferFromFileDescriptorWriteFailed, "Number of times the write (write/pwrite) to a file descriptor have failed.", ValueType::NUMBER) \
    M(WriteBufferFromFileDescriptorWriteBytes, "Number of bytes written to file descriptors. If the file is compressed, this will show compressed data size.", ValueType::BYTES) \
    M(FileSync, "Number of times the F_FULLFSYNC/fsync/fdatasync function was called for files.", ValueType::NUMBER) \
    M(DirectorySync, "Number of times the F_FULLFSYNC/fsync/fdatasync function was called for directories.", ValueType::NUMBER) \
    M(FileSyncElapsedMicroseconds, "Total time spent waiting for F_FULLFSYNC/fsync/fdatasync syscall for files.", ValueType::MICROSECONDS) \
    M(DirectorySyncElapsedMicroseconds, "Total time spent waiting for F_FULLFSYNC/fsync/fdatasync syscall for directories.", ValueType::MICROSECONDS) \
    M(ReadCompressedBytes, "Number of bytes (the number of bytes before decompression) read from compressed sources (files, network).", ValueType::BYTES) \
    M(CompressedReadBufferBlocks, "Number of compressed blocks (the blocks of data that are compressed independent of each other) read from compressed sources (files, network).", ValueType::NUMBER) \
    M(CompressedReadBufferBytes, "Number of uncompressed bytes (the number of bytes after decompression) read from compressed sources (files, network).", ValueType::BYTES) \
    M(UncompressedCacheHits, "Number of times a block of data has been found in the uncompressed cache (and decompression was avoided).", ValueType::NUMBER) \
    M(UncompressedCacheMisses, "Number of times a block of data has not been found in the uncompressed cache (and required decompression).", ValueType::NUMBER) \
    M(UncompressedCacheWeightLost, "Number of bytes evicted from the uncompressed cache.", ValueType::BYTES) \
    M(MMappedFileCacheHits, "Number of times a file has been found in the MMap cache (for the 'mmap' read_method), so we didn't have to mmap it again.", ValueType::NUMBER) \
    M(MMappedFileCacheMisses, "Number of times a file has not been found in the MMap cache (for the 'mmap' read_method), so we had to mmap it again.", ValueType::NUMBER) \
    M(OpenedFileCacheHits, "Number of times a file has been found in the opened file cache, so we didn't have to open it again.", ValueType::NUMBER) \
    M(OpenedFileCacheMisses, "Number of times a file has been found in the opened file cache, so we had to open it again.", ValueType::NUMBER) \
    M(OpenedFileCacheMicroseconds, "Amount of time spent executing OpenedFileCache methods.", ValueType::MICROSECONDS) \
    M(AIOWrite, "Number of writes with Linux or FreeBSD AIO interface", ValueType::NUMBER) \
    M(AIOWriteBytes, "Number of bytes written with Linux or FreeBSD AIO interface", ValueType::BYTES) \
    M(AIORead, "Number of reads with Linux or FreeBSD AIO interface", ValueType::NUMBER) \
    M(AIOReadBytes, "Number of bytes read with Linux or FreeBSD AIO interface", ValueType::BYTES) \
    M(IOBufferAllocs, "Number of allocations of IO buffers (for ReadBuffer/WriteBuffer).", ValueType::NUMBER) \
    M(IOBufferAllocBytes, "Number of bytes allocated for IO buffers (for ReadBuffer/WriteBuffer).", ValueType::BYTES) \
    M(ArenaAllocChunks, "Number of chunks allocated for memory Arena (used for GROUP BY and similar operations)", ValueType::NUMBER) \
    M(ArenaAllocBytes, "Number of bytes allocated for memory Arena (used for GROUP BY and similar operations)", ValueType::BYTES) \
    M(FunctionExecute, "Number of SQL ordinary function calls (SQL functions are called on per-block basis, so this number represents the number of blocks).", ValueType::NUMBER) \
    M(TableFunctionExecute, "Number of table function calls.", ValueType::NUMBER) \
    M(MarkCacheHits, "Number of times an entry has been found in the mark cache, so we didn't have to load a mark file.", ValueType::NUMBER) \
    M(MarkCacheMisses, "Number of times an entry has not been found in the mark cache, so we had to load a mark file in memory, which is a costly operation, adding to query latency.", ValueType::NUMBER) \
    M(QueryCacheHits, "Number of times a query result has been found in the query cache (and query computation was avoided). Only updated for SELECT queries with SETTING use_query_cache = 1.", ValueType::NUMBER) \
    M(QueryCacheMisses, "Number of times a query result has not been found in the query cache (and required query computation). Only updated for SELECT queries with SETTING use_query_cache = 1.", ValueType::NUMBER) \
    /* Each page cache chunk access increments exactly one of the following 5 PageCacheChunk* counters. */ \
    /* Something like hit rate: (PageCacheChunkShared + PageCacheChunkDataHits) / [sum of all 5]. */ \
    M(PageCacheChunkMisses, "Number of times a chunk has not been found in the userspace page cache.", ValueType::NUMBER) \
    M(PageCacheChunkShared, "Number of times a chunk has been found in the userspace page cache, already in use by another thread.", ValueType::NUMBER) \
    M(PageCacheChunkDataHits, "Number of times a chunk has been found in the userspace page cache, not in use, with all pages intact.", ValueType::NUMBER) \
    M(PageCacheChunkDataPartialHits, "Number of times a chunk has been found in the userspace page cache, not in use, but some of its pages were evicted by the OS.", ValueType::NUMBER) \
    M(PageCacheChunkDataMisses, "Number of times a chunk has been found in the userspace page cache, not in use, but all its pages were evicted by the OS.", ValueType::NUMBER) \
    M(PageCacheBytesUnpinnedRoundedToPages, "Total size of populated pages in chunks that became evictable in PageCache. Rounded up to whole pages.", ValueType::NUMBER) \
    M(PageCacheBytesUnpinnedRoundedToHugePages, "See PageCacheBytesUnpinnedRoundedToPages, but rounded to huge pages. Use the ratio between the two as a measure of memory waste from using huge pages.", ValueType::NUMBER) \
    M(CreatedReadBufferOrdinary, "Number of times ordinary read buffer was created for reading data (while choosing among other read methods).", ValueType::NUMBER) \
    M(CreatedReadBufferDirectIO, "Number of times a read buffer with O_DIRECT was created for reading data (while choosing among other read methods).", ValueType::NUMBER) \
    M(CreatedReadBufferDirectIOFailed, "Number of times a read buffer with O_DIRECT was attempted to be created for reading data (while choosing among other read methods), but the OS did not allow it (due to lack of filesystem support or other reasons) and we fallen back to the ordinary reading method.", ValueType::NUMBER) \
    M(CreatedReadBufferMMap, "Number of times a read buffer using 'mmap' was created for reading data (while choosing among other read methods).", ValueType::NUMBER) \
    M(CreatedReadBufferMMapFailed, "Number of times a read buffer with 'mmap' was attempted to be created for reading data (while choosing among other read methods), but the OS did not allow it (due to lack of filesystem support or other reasons) and we fallen back to the ordinary reading method.", ValueType::NUMBER) \
    M(DiskReadElapsedMicroseconds, "Total time spent waiting for read syscall. This include reads from page cache.", ValueType::MICROSECONDS) \
    M(DiskWriteElapsedMicroseconds, "Total time spent waiting for write syscall. This include writes to page cache.", ValueType::MICROSECONDS) \
    M(NetworkReceiveElapsedMicroseconds, "Total time spent waiting for data to receive or receiving data from network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.", ValueType::MICROSECONDS) \
    M(NetworkSendElapsedMicroseconds, "Total time spent waiting for data to send to network or sending data to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.", ValueType::MICROSECONDS) \
    M(NetworkReceiveBytes, "Total number of bytes received from network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.", ValueType::BYTES) \
    M(NetworkSendBytes, "Total number of bytes send to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.", ValueType::BYTES) \
    \
    M(DiskS3GetRequestThrottlerCount, "Number of DiskS3 GET and SELECT requests passed through throttler.", ValueType::NUMBER) \
    M(DiskS3GetRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform DiskS3 GET and SELECT request throttling.", ValueType::MICROSECONDS) \
    M(DiskS3PutRequestThrottlerCount, "Number of DiskS3 PUT, COPY, POST and LIST requests passed through throttler.", ValueType::NUMBER) \
    M(DiskS3PutRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform DiskS3 PUT, COPY, POST and LIST request throttling.", ValueType::MICROSECONDS) \
    M(S3GetRequestThrottlerCount, "Number of S3 GET and SELECT requests passed through throttler.", ValueType::NUMBER) \
    M(S3GetRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform S3 GET and SELECT request throttling.", ValueType::MICROSECONDS) \
    M(S3PutRequestThrottlerCount, "Number of S3 PUT, COPY, POST and LIST requests passed through throttler.", ValueType::NUMBER) \
    M(S3PutRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform S3 PUT, COPY, POST and LIST request throttling.", ValueType::MICROSECONDS) \
    M(RemoteReadThrottlerBytes, "Bytes passed through 'max_remote_read_network_bandwidth_for_server'/'max_remote_read_network_bandwidth' throttler.", ValueType::BYTES) \
    M(RemoteReadThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_remote_read_network_bandwidth_for_server'/'max_remote_read_network_bandwidth' throttling.", ValueType::MICROSECONDS) \
    M(RemoteWriteThrottlerBytes, "Bytes passed through 'max_remote_write_network_bandwidth_for_server'/'max_remote_write_network_bandwidth' throttler.", ValueType::BYTES) \
    M(RemoteWriteThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_remote_write_network_bandwidth_for_server'/'max_remote_write_network_bandwidth' throttling.", ValueType::MICROSECONDS) \
    M(LocalReadThrottlerBytes, "Bytes passed through 'max_local_read_bandwidth_for_server'/'max_local_read_bandwidth' throttler.", ValueType::BYTES) \
    M(LocalReadThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_local_read_bandwidth_for_server'/'max_local_read_bandwidth' throttling.", ValueType::MICROSECONDS) \
    M(LocalWriteThrottlerBytes, "Bytes passed through 'max_local_write_bandwidth_for_server'/'max_local_write_bandwidth' throttler.", ValueType::BYTES) \
    M(LocalWriteThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_local_write_bandwidth_for_server'/'max_local_write_bandwidth' throttling.", ValueType::MICROSECONDS) \
    M(ThrottlerSleepMicroseconds, "Total time a query was sleeping to conform all throttling settings.", ValueType::MICROSECONDS) \
    M(PartsWithAppliedMutationsOnFly, "Total number of parts for which there was any mutation applied on fly", ValueType::NUMBER) \
    M(MutationsAppliedOnFlyInAllParts, "The sum of number of applied mutations on-fly for part among all read parts", ValueType::NUMBER) \
    \
    M(QueryMaskingRulesMatch, "Number of times query masking rules was successfully matched.", ValueType::NUMBER) \
    \
    M(ReplicatedPartFetches, "Number of times a data part was downloaded from replica of a ReplicatedMergeTree table.", ValueType::NUMBER) \
    M(ReplicatedPartFailedFetches, "Number of times a data part was failed to download from replica of a ReplicatedMergeTree table.", ValueType::NUMBER) \
    M(ObsoleteReplicatedParts, "Number of times a data part was covered by another data part that has been fetched from a replica (so, we have marked a covered data part as obsolete and no longer needed).", ValueType::NUMBER) \
    M(ReplicatedPartMerges, "Number of times data parts of ReplicatedMergeTree tables were successfully merged.", ValueType::NUMBER) \
    M(ReplicatedPartFetchesOfMerged, "Number of times we prefer to download already merged part from replica of ReplicatedMergeTree table instead of performing a merge ourself (usually we prefer doing a merge ourself to save network traffic). This happens when we have not all source parts to perform a merge or when the data part is old enough.", ValueType::NUMBER) \
    M(ReplicatedPartMutations, "Number of times data parts of ReplicatedMergeTree tables were successfully mutated.", ValueType::NUMBER) \
    M(ReplicatedPartChecks, "Number of times we had to perform advanced search for a data part on replicas or to clarify the need of an existing data part.", ValueType::NUMBER) \
    M(ReplicatedPartChecksFailed, "Number of times the advanced search for a data part on replicas did not give result or when unexpected part has been found and moved away.", ValueType::NUMBER) \
    M(ReplicatedDataLoss, "Number of times a data part that we wanted doesn't exist on any replica (even on replicas that are offline right now). That data parts are definitely lost. This is normal due to asynchronous replication (if quorum inserts were not enabled), when the replica on which the data part was written was failed and when it became online after fail it doesn't contain that data part.", ValueType::NUMBER) \
    M(ReplicatedCoveredPartsInZooKeeperOnStart, "For debugging purposes. Number of parts in ZooKeeper that have a covering part, but doesn't exist on disk. Checked on server start.", ValueType::NUMBER) \
    \
    M(InsertedRows, "Number of rows INSERTed to all tables.", ValueType::NUMBER) \
    M(InsertedBytes, "Number of bytes (uncompressed; for columns as they stored in memory) INSERTed to all tables.", ValueType::BYTES) \
    M(DelayedInserts, "Number of times the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.", ValueType::NUMBER) \
    M(RejectedInserts, "Number of times the INSERT of a block to a MergeTree table was rejected with 'Too many parts' exception due to high number of active data parts for partition.", ValueType::NUMBER) \
    M(DelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.", ValueType::MILLISECONDS) \
    M(DelayedMutations, "Number of times the mutation of a MergeTree table was throttled due to high number of unfinished mutations for table.", ValueType::NUMBER) \
    M(RejectedMutations, "Number of times the mutation of a MergeTree table was rejected with 'Too many mutations' exception due to high number of unfinished mutations for table.", ValueType::NUMBER) \
    M(DelayedMutationsMilliseconds, "Total number of milliseconds spent while the mutation of a MergeTree table was throttled due to high number of unfinished mutations for table.", ValueType::MILLISECONDS) \
    M(DistributedDelayedInserts, "Number of times the INSERT of a block to a Distributed table was throttled due to high number of pending bytes.", ValueType::NUMBER) \
    M(DistributedRejectedInserts, "Number of times the INSERT of a block to a Distributed table was rejected with 'Too many bytes' exception due to high number of pending bytes.", ValueType::NUMBER) \
    M(DistributedDelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a Distributed table was throttled due to high number of pending bytes.", ValueType::MILLISECONDS) \
    M(DuplicatedInsertedBlocks, "Number of times the INSERTed block to a ReplicatedMergeTree table was deduplicated.", ValueType::NUMBER) \
    \
    M(ZooKeeperInit, "Number of times connection with ZooKeeper has been established.", ValueType::NUMBER) \
    M(ZooKeeperTransactions, "Number of ZooKeeper operations, which include both read and write operations as well as multi-transactions.", ValueType::NUMBER) \
    M(ZooKeeperList, "Number of 'list' (getChildren) requests to ZooKeeper.", ValueType::NUMBER) \
    M(ZooKeeperCreate, "Number of 'create' requests to ZooKeeper.", ValueType::NUMBER) \
    M(ZooKeeperRemove, "Number of 'remove' requests to ZooKeeper.", ValueType::NUMBER) \
    M(ZooKeeperExists, "Number of 'exists' requests to ZooKeeper.", ValueType::NUMBER) \
    M(ZooKeeperGet, "Number of 'get' requests to ZooKeeper.", ValueType::NUMBER) \
    M(ZooKeeperSet, "Number of 'set' requests to ZooKeeper.", ValueType::NUMBER) \
    M(ZooKeeperMulti, "Number of 'multi' requests to ZooKeeper (compound transactions).", ValueType::NUMBER) \
    M(ZooKeeperCheck, "Number of 'check' requests to ZooKeeper. Usually they don't make sense in isolation, only as part of a complex transaction.", ValueType::NUMBER) \
    M(ZooKeeperSync, "Number of 'sync' requests to ZooKeeper. These requests are rarely needed or usable.", ValueType::NUMBER) \
    M(ZooKeeperReconfig, "Number of 'reconfig' requests to ZooKeeper.", ValueType::NUMBER) \
    M(ZooKeeperClose, "Number of times connection with ZooKeeper has been closed voluntary.", ValueType::NUMBER) \
    M(ZooKeeperWatchResponse, "Number of times watch notification has been received from ZooKeeper.", ValueType::NUMBER) \
    M(ZooKeeperUserExceptions, "Number of exceptions while working with ZooKeeper related to the data (no node, bad version or similar).", ValueType::NUMBER) \
    M(ZooKeeperHardwareExceptions, "Number of exceptions while working with ZooKeeper related to network (connection loss or similar).", ValueType::NUMBER) \
    M(ZooKeeperOtherExceptions, "Number of exceptions while working with ZooKeeper other than ZooKeeperUserExceptions and ZooKeeperHardwareExceptions.", ValueType::NUMBER) \
    M(ZooKeeperWaitMicroseconds, "Number of microseconds spent waiting for responses from ZooKeeper after creating a request, summed across all the requesting threads.", ValueType::MICROSECONDS) \
    M(ZooKeeperBytesSent, "Number of bytes send over network while communicating with ZooKeeper.", ValueType::BYTES) \
    M(ZooKeeperBytesReceived, "Number of bytes received over network while communicating with ZooKeeper.", ValueType::BYTES) \
    \
    M(DistributedConnectionTries, "Total count of distributed connection attempts.", ValueType::NUMBER) \
    M(DistributedConnectionUsable, "Total count of successful distributed connections to a usable server (with required table, but maybe stale).", ValueType::NUMBER) \
    M(DistributedConnectionFailTry, "Total count when distributed connection fails with retry.", ValueType::NUMBER) \
    M(DistributedConnectionMissingTable, "Number of times we rejected a replica from a distributed query, because it did not contain a table needed for the query.", ValueType::NUMBER) \
    M(DistributedConnectionStaleReplica, "Number of times we rejected a replica from a distributed query, because some table needed for a query had replication lag higher than the configured threshold.", ValueType::NUMBER) \
    M(DistributedConnectionSkipReadOnlyReplica, "Number of replicas skipped during INSERT into Distributed table due to replicas being read-only", ValueType::NUMBER) \
    M(DistributedConnectionFailAtAll, "Total count when distributed connection fails after all retries finished.", ValueType::NUMBER) \
    \
    M(HedgedRequestsChangeReplica, "Total count when timeout for changing replica expired in hedged requests.", ValueType::NUMBER) \
    M(SuspendSendingQueryToShard, "Total count when sending query to shard was suspended when async_query_sending_for_remote is enabled.", ValueType::NUMBER) \
    \
    M(CompileFunction, "Number of times a compilation of generated LLVM code (to create fused function for complex expressions) was initiated.", ValueType::NUMBER) \
    M(CompiledFunctionExecute, "Number of times a compiled function was executed.", ValueType::NUMBER) \
    M(CompileExpressionsMicroseconds, "Total time spent for compilation of expressions to LLVM code.", ValueType::MICROSECONDS) \
    M(CompileExpressionsBytes, "Number of bytes used for expressions compilation.", ValueType::BYTES) \
    \
    M(ExecuteShellCommand, "Number of shell command executions.", ValueType::NUMBER) \
    \
    M(ExternalProcessingCompressedBytesTotal, "Number of compressed bytes written by external processing (sorting/aggragating/joining)", ValueType::BYTES) \
    M(ExternalProcessingUncompressedBytesTotal, "Amount of data (uncompressed, before compression) written by external processing (sorting/aggragating/joining)", ValueType::BYTES) \
    M(ExternalProcessingFilesTotal, "Number of files used by external processing (sorting/aggragating/joining)", ValueType::NUMBER) \
    M(ExternalSortWritePart, "Number of times a temporary file was written to disk for sorting in external memory.", ValueType::NUMBER) \
    M(ExternalSortMerge, "Number of times temporary files were merged for sorting in external memory.", ValueType::NUMBER) \
    M(ExternalSortCompressedBytes, "Number of compressed bytes written for sorting in external memory.", ValueType::BYTES) \
    M(ExternalSortUncompressedBytes, "Amount of data (uncompressed, before compression) written for sorting in external memory.", ValueType::BYTES) \
    M(ExternalAggregationWritePart, "Number of times a temporary file was written to disk for aggregation in external memory.", ValueType::NUMBER) \
    M(ExternalAggregationMerge, "Number of times temporary files were merged for aggregation in external memory.", ValueType::NUMBER) \
    M(ExternalAggregationCompressedBytes, "Number of bytes written to disk for aggregation in external memory.", ValueType::BYTES) \
    M(ExternalAggregationUncompressedBytes, "Amount of data (uncompressed, before compression) written to disk for aggregation in external memory.", ValueType::BYTES) \
    M(ExternalJoinWritePart, "Number of times a temporary file was written to disk for JOIN in external memory.", ValueType::NUMBER) \
    M(ExternalJoinMerge, "Number of times temporary files were merged for JOIN in external memory.", ValueType::NUMBER) \
    M(ExternalJoinCompressedBytes, "Number of compressed bytes written for JOIN in external memory.", ValueType::BYTES) \
    M(ExternalJoinUncompressedBytes, "Amount of data (uncompressed, before compression) written for JOIN in external memory.", ValueType::BYTES) \
    \
    M(SlowRead, "Number of reads from a file that were slow. This indicate system overload. Thresholds are controlled by read_backoff_* settings.", ValueType::NUMBER) \
    M(ReadBackoff, "Number of times the number of query processing threads was lowered due to slow reads.", ValueType::NUMBER) \
    \
    M(ReplicaPartialShutdown, "How many times Replicated table has to deinitialize its state due to session expiration in ZooKeeper. The state is reinitialized every time when ZooKeeper is available again.", ValueType::NUMBER) \
    \
    M(SelectedParts, "Number of data parts selected to read from a MergeTree table.", ValueType::NUMBER) \
    M(SelectedRanges, "Number of (non-adjacent) ranges in all data parts selected to read from a MergeTree table.", ValueType::NUMBER) \
    M(SelectedMarks, "Number of marks (index granules) selected to read from a MergeTree table.", ValueType::NUMBER) \
    M(SelectedRows, "Number of rows SELECTed from all tables.", ValueType::NUMBER) \
    M(SelectedBytes, "Number of bytes (uncompressed; for columns as they stored in memory) SELECTed from all tables.", ValueType::BYTES) \
    \
    M(WaitMarksLoadMicroseconds, "Time spent loading marks", ValueType::MICROSECONDS) \
    M(BackgroundLoadingMarksTasks, "Number of background tasks for loading marks", ValueType::NUMBER) \
    M(LoadedMarksCount, "Number of marks loaded (total across columns).", ValueType::NUMBER) \
    M(LoadedMarksMemoryBytes, "Size of in-memory representations of loaded marks.", ValueType::BYTES) \
    \
    M(Merge, "Number of launched background merges.", ValueType::NUMBER) \
    M(MergedRows, "Rows read for background merges. This is the number of rows before merge.", ValueType::NUMBER) \
    M(MergedUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) that was read for background merges. This is the number before merge.", ValueType::BYTES) \
    M(MergesTimeMilliseconds, "Total time spent for background merges.", ValueType::MILLISECONDS) \
    \
    M(MergeTreeDataWriterRows, "Number of rows INSERTed to MergeTree tables.", ValueType::NUMBER) \
    M(MergeTreeDataWriterUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables.", ValueType::BYTES) \
    M(MergeTreeDataWriterCompressedBytes, "Bytes written to filesystem for data INSERTed to MergeTree tables.", ValueType::BYTES) \
    M(MergeTreeDataWriterBlocks, "Number of blocks INSERTed to MergeTree tables. Each block forms a data part of level zero.", ValueType::NUMBER) \
    M(MergeTreeDataWriterBlocksAlreadySorted, "Number of blocks INSERTed to MergeTree tables that appeared to be already sorted.", ValueType::NUMBER) \
    \
    M(MergeTreeDataWriterSkipIndicesCalculationMicroseconds, "Time spent calculating skip indices", ValueType::MICROSECONDS) \
    M(MergeTreeDataWriterStatisticsCalculationMicroseconds, "Time spent calculating statistics", ValueType::MICROSECONDS) \
    M(MergeTreeDataWriterSortingBlocksMicroseconds, "Time spent sorting blocks", ValueType::MICROSECONDS) \
    M(MergeTreeDataWriterMergingBlocksMicroseconds, "Time spent merging input blocks (for special MergeTree engines)", ValueType::MICROSECONDS) \
    M(MergeTreeDataWriterProjectionsCalculationMicroseconds, "Time spent calculating projections", ValueType::MICROSECONDS) \
    M(MergeTreeDataProjectionWriterSortingBlocksMicroseconds, "Time spent sorting blocks (for projection it might be a key different from table's sorting key)", ValueType::MICROSECONDS) \
    M(MergeTreeDataProjectionWriterMergingBlocksMicroseconds, "Time spent merging blocks", ValueType::MICROSECONDS) \
    M(MutateTaskProjectionsCalculationMicroseconds, "Time spent calculating projections", ValueType::MICROSECONDS) \
    \
    M(InsertedWideParts, "Number of parts inserted in Wide format.", ValueType::NUMBER) \
    M(InsertedCompactParts, "Number of parts inserted in Compact format.", ValueType::NUMBER) \
    M(MergedIntoWideParts, "Number of parts merged into Wide format.", ValueType::NUMBER) \
    M(MergedIntoCompactParts, "Number of parts merged into Compact format.", ValueType::NUMBER) \
    \
    M(MergeTreeDataProjectionWriterRows, "Number of rows INSERTed to MergeTree tables projection.", ValueType::NUMBER) \
    M(MergeTreeDataProjectionWriterUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables projection.", ValueType::BYTES) \
    M(MergeTreeDataProjectionWriterCompressedBytes, "Bytes written to filesystem for data INSERTed to MergeTree tables projection.", ValueType::BYTES) \
    M(MergeTreeDataProjectionWriterBlocks, "Number of blocks INSERTed to MergeTree tables projection. Each block forms a data part of level zero.", ValueType::NUMBER) \
    M(MergeTreeDataProjectionWriterBlocksAlreadySorted, "Number of blocks INSERTed to MergeTree tables projection that appeared to be already sorted.", ValueType::NUMBER) \
    \
    M(CannotRemoveEphemeralNode, "Number of times an error happened while trying to remove ephemeral node. This is not an issue, because our implementation of ZooKeeper library guarantee that the session will expire and the node will be removed.", ValueType::NUMBER) \
    \
    M(RegexpCreated, "Compiled regular expressions. Identical regular expressions compiled just once and cached forever.", ValueType::NUMBER) \
    M(ContextLock, "Number of times the lock of Context was acquired or tried to acquire. This is global lock.", ValueType::NUMBER) \
    M(ContextLockWaitMicroseconds, "Context lock wait time in microseconds", ValueType::MICROSECONDS) \
    \
    M(StorageBufferFlush, "Number of times a buffer in a 'Buffer' table was flushed.", ValueType::NUMBER) \
    M(StorageBufferErrorOnFlush, "Number of times a buffer in the 'Buffer' table has not been able to flush due to error writing in the destination table.", ValueType::NUMBER) \
    M(StorageBufferPassedAllMinThresholds, "Number of times a criteria on min thresholds has been reached to flush a buffer in a 'Buffer' table.", ValueType::NUMBER) \
    M(StorageBufferPassedTimeMaxThreshold, "Number of times a criteria on max time threshold has been reached to flush a buffer in a 'Buffer' table.", ValueType::NUMBER) \
    M(StorageBufferPassedRowsMaxThreshold, "Number of times a criteria on max rows threshold has been reached to flush a buffer in a 'Buffer' table.", ValueType::NUMBER) \
    M(StorageBufferPassedBytesMaxThreshold, "Number of times a criteria on max bytes threshold has been reached to flush a buffer in a 'Buffer' table.", ValueType::NUMBER) \
    M(StorageBufferPassedTimeFlushThreshold, "Number of times background-only flush threshold on time has been reached to flush a buffer in a 'Buffer' table. This is expert-only metric. If you read this and you are not an expert, stop reading.", ValueType::NUMBER) \
    M(StorageBufferPassedRowsFlushThreshold, "Number of times background-only flush threshold on rows has been reached to flush a buffer in a 'Buffer' table. This is expert-only metric. If you read this and you are not an expert, stop reading.", ValueType::NUMBER) \
    M(StorageBufferPassedBytesFlushThreshold, "Number of times background-only flush threshold on bytes has been reached to flush a buffer in a 'Buffer' table. This is expert-only metric. If you read this and you are not an expert, stop reading.", ValueType::NUMBER) \
    M(StorageBufferLayerLockReadersWaitMilliseconds, "Time for waiting for Buffer layer during reading.", ValueType::MILLISECONDS) \
    M(StorageBufferLayerLockWritersWaitMilliseconds, "Time for waiting free Buffer layer to write to (can be used to tune Buffer layers).", ValueType::MILLISECONDS) \
    \
    M(DictCacheKeysRequested, "Number of keys requested from the data source for the dictionaries of 'cache' types.", ValueType::NUMBER) \
    M(DictCacheKeysRequestedMiss, "Number of keys requested from the data source for dictionaries of 'cache' types but not found in the data source.", ValueType::NUMBER) \
    M(DictCacheKeysRequestedFound, "Number of keys requested from the data source for dictionaries of 'cache' types and found in the data source.", ValueType::NUMBER) \
    M(DictCacheKeysExpired, "Number of keys looked up in the dictionaries of 'cache' types and found in the cache but they were obsolete.", ValueType::NUMBER) \
    M(DictCacheKeysNotFound, "Number of keys looked up in the dictionaries of 'cache' types and not found.", ValueType::NUMBER) \
    M(DictCacheKeysHit, "Number of keys looked up in the dictionaries of 'cache' types and found in the cache.", ValueType::NUMBER) \
    M(DictCacheRequestTimeNs, "Number of nanoseconds spend in querying the external data sources for the dictionaries of 'cache' types.", ValueType::NANOSECONDS) \
    M(DictCacheRequests, "Number of bulk requests to the external data sources for the dictionaries of 'cache' types.", ValueType::NUMBER) \
    M(DictCacheLockWriteNs, "Number of nanoseconds spend in waiting for write lock to update the data for the dictionaries of 'cache' types.", ValueType::NANOSECONDS) \
    M(DictCacheLockReadNs, "Number of nanoseconds spend in waiting for read lock to lookup the data for the dictionaries of 'cache' types.", ValueType::NANOSECONDS) \
    \
    M(DistributedSyncInsertionTimeoutExceeded, "A timeout has exceeded while waiting for shards during synchronous insertion into a Distributed table (with 'distributed_foreground_insert' = 1)", ValueType::NUMBER) \
    M(DistributedAsyncInsertionFailures, "Number of failures for asynchronous insertion into a Distributed table (with 'distributed_foreground_insert' = 0)", ValueType::NUMBER) \
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
)", ValueType::NUMBER) \
    M(DataAfterMutationDiffersFromReplica, "Number of times data after mutation is not byte-identical to the data on other replicas. In addition to the reasons described in 'DataAfterMergeDiffersFromReplica', it is also possible due to non-deterministic mutation.", ValueType::NUMBER) \
    M(PolygonsAddedToPool, "A polygon has been added to the cache (pool) for the 'pointInPolygon' function.", ValueType::NUMBER) \
    M(PolygonsInPoolAllocatedBytes, "The number of bytes for polygons added to the cache (pool) for the 'pointInPolygon' function.", ValueType::BYTES) \
    \
    M(USearchAddCount, "Number of vectors added to usearch indexes.", ValueType::NUMBER) \
    M(USearchAddVisitedMembers, "Number of nodes visited when adding vectors to usearch indexes.", ValueType::NUMBER) \
    M(USearchAddComputedDistances, "Number of times distance was computed when adding vectors to usearch indexes.", ValueType::NUMBER) \
    M(USearchSearchCount, "Number of search operations performed in usearch indexes.", ValueType::NUMBER) \
    M(USearchSearchVisitedMembers, "Number of nodes visited when searching in usearch indexes.", ValueType::NUMBER) \
    M(USearchSearchComputedDistances, "Number of times distance was computed when searching usearch indexes.", ValueType::NUMBER) \
    \
    M(RWLockAcquiredReadLocks, "Number of times a read lock was acquired (in a heavy RWLock).", ValueType::NUMBER) \
    M(RWLockAcquiredWriteLocks, "Number of times a write lock was acquired (in a heavy RWLock).", ValueType::NUMBER) \
    M(RWLockReadersWaitMilliseconds, "Total time spent waiting for a read lock to be acquired (in a heavy RWLock).", ValueType::MILLISECONDS) \
    M(RWLockWritersWaitMilliseconds, "Total time spent waiting for a write lock to be acquired (in a heavy RWLock).", ValueType::MILLISECONDS) \
    M(DNSError, "Total count of errors in DNS resolution", ValueType::NUMBER) \
    M(PartsLockHoldMicroseconds, "Total time spent holding data parts lock in MergeTree tables", ValueType::MICROSECONDS) \
    M(PartsLockWaitMicroseconds, "Total time spent waiting for data parts lock in MergeTree tables", ValueType::MICROSECONDS) \
    \
    M(RealTimeMicroseconds, "Total (wall clock) time spent in processing (queries and other tasks) threads (note that this is a sum).", ValueType::MICROSECONDS) \
    M(UserTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in user mode. This includes time CPU pipeline was stalled due to main memory access, cache misses, branch mispredictions, hyper-threading, etc.", ValueType::MICROSECONDS) \
    M(SystemTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in OS kernel mode. This is time spent in syscalls, excluding waiting time during blocking syscalls.", ValueType::MICROSECONDS) \
    M(MemoryOvercommitWaitTimeMicroseconds, "Total time spent in waiting for memory to be freed in OvercommitTracker.", ValueType::MICROSECONDS) \
    M(MemoryAllocatorPurge, "Total number of times memory allocator purge was requested", ValueType::NUMBER) \
    M(MemoryAllocatorPurgeTimeMicroseconds, "Total number of times memory allocator purge was requested", ValueType::MICROSECONDS) \
    M(SoftPageFaults, "The number of soft page faults in query execution threads. Soft page fault usually means a miss in the memory allocator cache, which requires a new memory mapping from the OS and subsequent allocation of a page of physical memory.", ValueType::NUMBER) \
    M(HardPageFaults, "The number of hard page faults in query execution threads. High values indicate either that you forgot to turn off swap on your server, or eviction of memory pages of the ClickHouse binary during very high memory pressure, or successful usage of the 'mmap' read method for the tables data.", ValueType::NUMBER) \
    \
    M(OSIOWaitMicroseconds, "Total time a thread spent waiting for a result of IO operation, from the OS point of view. This is real IO that doesn't include page cache.", ValueType::MICROSECONDS) \
    M(OSCPUWaitMicroseconds, "Total time a thread was ready for execution but waiting to be scheduled by OS, from the OS point of view.", ValueType::MICROSECONDS) \
    M(OSCPUVirtualTimeMicroseconds, "CPU time spent seen by OS. Does not include involuntary waits due to virtualization.", ValueType::MICROSECONDS) \
    M(OSReadBytes, "Number of bytes read from disks or block devices. Doesn't include bytes read from page cache. May include excessive data due to block size, readahead, etc.", ValueType::BYTES) \
    M(OSWriteBytes, "Number of bytes written to disks or block devices. Doesn't include bytes that are in page cache dirty pages. May not include data that was written by OS asynchronously.", ValueType::BYTES) \
    M(OSReadChars, "Number of bytes read from filesystem, including page cache.", ValueType::BYTES) \
    M(OSWriteChars, "Number of bytes written to filesystem, including page cache.", ValueType::BYTES) \
    \
    M(ParallelReplicasHandleRequestMicroseconds, "Time spent processing requests for marks from replicas", ValueType::MICROSECONDS) \
    M(ParallelReplicasHandleAnnouncementMicroseconds, "Time spent processing replicas announcements", ValueType::MICROSECONDS) \
    \
    M(ParallelReplicasReadAssignedMarks, "Sum across all replicas of how many of scheduled marks were assigned by consistent hash", ValueType::NUMBER) \
    M(ParallelReplicasReadUnassignedMarks, "Sum across all replicas of how many unassigned marks were scheduled", ValueType::NUMBER) \
    M(ParallelReplicasReadAssignedForStealingMarks, "Sum across all replicas of how many of scheduled marks were assigned for stealing by consistent hash", ValueType::NUMBER) \
    \
    M(ParallelReplicasStealingByHashMicroseconds, "Time spent collecting segments meant for stealing by hash", ValueType::MICROSECONDS) \
    M(ParallelReplicasProcessingPartsMicroseconds, "Time spent processing data parts", ValueType::MICROSECONDS) \
    M(ParallelReplicasStealingLeftoversMicroseconds, "Time spent collecting orphaned segments", ValueType::MICROSECONDS) \
    M(ParallelReplicasCollectingOwnedSegmentsMicroseconds, "Time spent collecting segments meant by hash", ValueType::MICROSECONDS) \
    M(ParallelReplicasNumRequests, "Number of requests to the initiator.", ValueType::NUMBER) \
    M(ParallelReplicasDeniedRequests, "Number of completely denied requests to the initiator", ValueType::NUMBER) \
    M(CacheWarmerBytesDownloaded, "Amount of data fetched into filesystem cache by dedicated background threads.", ValueType::BYTES) \
    M(CacheWarmerDataPartsDownloaded, "Number of data parts that were fully fetched by CacheWarmer.", ValueType::NUMBER) \
    M(IgnoredColdParts, "See setting ignore_cold_parts_seconds. Number of times read queries ignored very new parts that weren't pulled into cache by CacheWarmer yet.", ValueType::NUMBER) \
    M(PreferredWarmedUnmergedParts, "See setting prefer_warmed_unmerged_parts_seconds. Number of times read queries used outdated pre-merge parts that are in cache instead of merged part that wasn't pulled into cache by CacheWarmer yet.", ValueType::NUMBER) \
    \
    M(PerfCPUCycles, "Total cycles. Be wary of what happens during CPU frequency scaling.", ValueType::NUMBER) \
    M(PerfInstructions, "Retired instructions. Be careful, these can be affected by various issues, most notably hardware interrupt counts.", ValueType::NUMBER) \
    M(PerfCacheReferences, "Cache accesses. Usually, this indicates Last Level Cache accesses, but this may vary depending on your CPU. This may include prefetches and coherency messages; again this depends on the design of your CPU.", ValueType::NUMBER) \
    M(PerfCacheMisses, "Cache misses. Usually this indicates Last Level Cache misses; this is intended to be used in conjunction with the PERFCOUNTHWCACHEREFERENCES event to calculate cache miss rates.", ValueType::NUMBER) \
    M(PerfBranchInstructions, "Retired branch instructions. Prior to Linux 2.6.35, this used the wrong event on AMD processors.", ValueType::NUMBER) \
    M(PerfBranchMisses, "Mispredicted branch instructions.", ValueType::NUMBER) \
    M(PerfBusCycles, "Bus cycles, which can be different from total cycles.", ValueType::NUMBER) \
    M(PerfStalledCyclesFrontend, "Stalled cycles during issue.", ValueType::NUMBER) \
    M(PerfStalledCyclesBackend, "Stalled cycles during retirement.", ValueType::NUMBER) \
    M(PerfRefCPUCycles, "Total cycles; not affected by CPU frequency scaling.", ValueType::NUMBER) \
    \
    M(PerfCPUClock, "The CPU clock, a high-resolution per-CPU timer", ValueType::NUMBER) \
    M(PerfTaskClock, "A clock count specific to the task that is running", ValueType::NUMBER) \
    M(PerfContextSwitches, "Number of context switches", ValueType::NUMBER) \
    M(PerfCPUMigrations, "Number of times the process has migrated to a new CPU", ValueType::NUMBER) \
    M(PerfAlignmentFaults, "Number of alignment faults. These happen when unaligned memory accesses happen; the kernel can handle these but it reduces performance. This happens only on some architectures (never on x86).", ValueType::NUMBER) \
    M(PerfEmulationFaults, "Number of emulation faults. The kernel sometimes traps on unimplemented instructions and emulates them for user space. This can negatively impact performance.", ValueType::NUMBER) \
    M(PerfMinEnabledTime, "For all events, minimum time that an event was enabled. Used to track event multiplexing influence", ValueType::NUMBER) \
    M(PerfMinEnabledRunningTime, "Running time for event with minimum enabled time. Used to track the amount of event multiplexing", ValueType::NUMBER) \
    M(PerfDataTLBReferences, "Data TLB references", ValueType::NUMBER) \
    M(PerfDataTLBMisses, "Data TLB misses", ValueType::NUMBER) \
    M(PerfInstructionTLBReferences, "Instruction TLB references", ValueType::NUMBER) \
    M(PerfInstructionTLBMisses, "Instruction TLB misses", ValueType::NUMBER) \
    M(PerfLocalMemoryReferences, "Local NUMA node memory reads", ValueType::NUMBER) \
    M(PerfLocalMemoryMisses, "Local NUMA node memory read misses", ValueType::NUMBER) \
    \
    M(CannotWriteToWriteBufferDiscard, "Number of stack traces dropped by query profiler or signal handler because pipe is full or cannot write to pipe.", ValueType::NUMBER) \
    M(QueryProfilerSignalOverruns, "Number of times we drop processing of a query profiler signal due to overrun plus the number of signals that OS has not delivered due to overrun.", ValueType::NUMBER) \
    M(QueryProfilerConcurrencyOverruns, "Number of times we drop processing of a query profiler signal due to too many concurrent query profilers in other threads, which may indicate overload.", ValueType::NUMBER) \
    M(QueryProfilerRuns, "Number of times QueryProfiler had been run.", ValueType::NUMBER) \
    \
    M(CreatedLogEntryForMerge, "Successfully created log entry to merge parts in ReplicatedMergeTree.", ValueType::NUMBER) \
    M(NotCreatedLogEntryForMerge, "Log entry to merge parts in ReplicatedMergeTree is not created due to concurrent log update by another replica.", ValueType::NUMBER) \
    M(CreatedLogEntryForMutation, "Successfully created log entry to mutate parts in ReplicatedMergeTree.", ValueType::NUMBER) \
    M(NotCreatedLogEntryForMutation, "Log entry to mutate parts in ReplicatedMergeTree is not created due to concurrent log update by another replica.", ValueType::NUMBER) \
    \
    M(S3ReadMicroseconds, "Time of GET and HEAD requests to S3 storage.", ValueType::MICROSECONDS) \
    M(S3ReadRequestsCount, "Number of GET and HEAD requests to S3 storage.", ValueType::NUMBER) \
    M(S3ReadRequestsErrors, "Number of non-throttling errors in GET and HEAD requests to S3 storage.", ValueType::NUMBER) \
    M(S3ReadRequestsThrottling, "Number of 429 and 503 errors in GET and HEAD requests to S3 storage.", ValueType::NUMBER) \
    M(S3ReadRequestsRedirects, "Number of redirects in GET and HEAD requests to S3 storage.", ValueType::NUMBER) \
    \
    M(S3WriteMicroseconds, "Time of POST, DELETE, PUT and PATCH requests to S3 storage.", ValueType::MICROSECONDS) \
    M(S3WriteRequestsCount, "Number of POST, DELETE, PUT and PATCH requests to S3 storage.", ValueType::NUMBER) \
    M(S3WriteRequestsErrors, "Number of non-throttling errors in POST, DELETE, PUT and PATCH requests to S3 storage.", ValueType::NUMBER) \
    M(S3WriteRequestsThrottling, "Number of 429 and 503 errors in POST, DELETE, PUT and PATCH requests to S3 storage.", ValueType::NUMBER) \
    M(S3WriteRequestsRedirects, "Number of redirects in POST, DELETE, PUT and PATCH requests to S3 storage.", ValueType::NUMBER) \
    \
    M(DiskS3ReadMicroseconds, "Time of GET and HEAD requests to DiskS3 storage.", ValueType::MICROSECONDS) \
    M(DiskS3ReadRequestsCount, "Number of GET and HEAD requests to DiskS3 storage.", ValueType::NUMBER) \
    M(DiskS3ReadRequestsErrors, "Number of non-throttling errors in GET and HEAD requests to DiskS3 storage.", ValueType::NUMBER) \
    M(DiskS3ReadRequestsThrottling, "Number of 429 and 503 errors in GET and HEAD requests to DiskS3 storage.", ValueType::NUMBER) \
    M(DiskS3ReadRequestsRedirects, "Number of redirects in GET and HEAD requests to DiskS3 storage.", ValueType::NUMBER) \
    \
    M(DiskS3WriteMicroseconds, "Time of POST, DELETE, PUT and PATCH requests to DiskS3 storage.", ValueType::MICROSECONDS) \
    M(DiskS3WriteRequestsCount, "Number of POST, DELETE, PUT and PATCH requests to DiskS3 storage.", ValueType::NUMBER) \
    M(DiskS3WriteRequestsErrors, "Number of non-throttling errors in POST, DELETE, PUT and PATCH requests to DiskS3 storage.", ValueType::NUMBER) \
    M(DiskS3WriteRequestsThrottling, "Number of 429 and 503 errors in POST, DELETE, PUT and PATCH requests to DiskS3 storage.", ValueType::NUMBER) \
    M(DiskS3WriteRequestsRedirects, "Number of redirects in POST, DELETE, PUT and PATCH requests to DiskS3 storage.", ValueType::NUMBER) \
    \
    M(S3DeleteObjects, "Number of S3 API DeleteObject(s) calls.", ValueType::NUMBER) \
    M(S3CopyObject, "Number of S3 API CopyObject calls.", ValueType::NUMBER) \
    M(S3ListObjects, "Number of S3 API ListObjects calls.", ValueType::NUMBER) \
    M(S3HeadObject,  "Number of S3 API HeadObject calls.", ValueType::NUMBER) \
    M(S3GetObjectAttributes, "Number of S3 API GetObjectAttributes calls.", ValueType::NUMBER) \
    M(S3CreateMultipartUpload, "Number of S3 API CreateMultipartUpload calls.", ValueType::NUMBER) \
    M(S3UploadPartCopy, "Number of S3 API UploadPartCopy calls.", ValueType::NUMBER) \
    M(S3UploadPart, "Number of S3 API UploadPart calls.", ValueType::NUMBER) \
    M(S3AbortMultipartUpload, "Number of S3 API AbortMultipartUpload calls.", ValueType::NUMBER) \
    M(S3CompleteMultipartUpload, "Number of S3 API CompleteMultipartUpload calls.", ValueType::NUMBER) \
    M(S3PutObject, "Number of S3 API PutObject calls.", ValueType::NUMBER) \
    M(S3GetObject, "Number of S3 API GetObject calls.", ValueType::NUMBER) \
    \
    M(DiskS3DeleteObjects, "Number of DiskS3 API DeleteObject(s) calls.", ValueType::NUMBER) \
    M(DiskS3CopyObject, "Number of DiskS3 API CopyObject calls.", ValueType::NUMBER) \
    M(DiskS3ListObjects, "Number of DiskS3 API ListObjects calls.", ValueType::NUMBER) \
    M(DiskS3HeadObject,  "Number of DiskS3 API HeadObject calls.", ValueType::NUMBER) \
    M(DiskS3GetObjectAttributes, "Number of DiskS3 API GetObjectAttributes calls.", ValueType::NUMBER) \
    M(DiskS3CreateMultipartUpload, "Number of DiskS3 API CreateMultipartUpload calls.", ValueType::NUMBER) \
    M(DiskS3UploadPartCopy, "Number of DiskS3 API UploadPartCopy calls.", ValueType::NUMBER) \
    M(DiskS3UploadPart, "Number of DiskS3 API UploadPart calls.", ValueType::NUMBER) \
    M(DiskS3AbortMultipartUpload, "Number of DiskS3 API AbortMultipartUpload calls.", ValueType::NUMBER) \
    M(DiskS3CompleteMultipartUpload, "Number of DiskS3 API CompleteMultipartUpload calls.", ValueType::NUMBER) \
    M(DiskS3PutObject, "Number of DiskS3 API PutObject calls.", ValueType::NUMBER) \
    M(DiskS3GetObject, "Number of DiskS3 API GetObject calls.", ValueType::NUMBER) \
    \
    M(S3Clients, "Number of created S3 clients.", ValueType::NUMBER) \
    M(TinyS3Clients, "Number of S3 clients copies which reuse an existing auth provider from another client.", ValueType::NUMBER) \
    \
    M(EngineFileLikeReadFiles, "Number of files read in table engines working with files (like File/S3/URL/HDFS).", ValueType::NUMBER) \
    \
    M(ReadBufferFromS3Microseconds, "Time spent on reading from S3.", ValueType::MICROSECONDS) \
    M(ReadBufferFromS3InitMicroseconds, "Time spent initializing connection to S3.", ValueType::MICROSECONDS) \
    M(ReadBufferFromS3Bytes, "Bytes read from S3.", ValueType::BYTES) \
    M(ReadBufferFromS3RequestsErrors, "Number of exceptions while reading from S3.", ValueType::NUMBER) \
    M(ReadBufferFromS3ResetSessions, "Number of HTTP sessions that were reset in ReadBufferFromS3.", ValueType::NUMBER) \
    M(ReadBufferFromS3PreservedSessions, "Number of HTTP sessions that were preserved in ReadBufferFromS3.", ValueType::NUMBER) \
    \
    M(WriteBufferFromS3Microseconds, "Time spent on writing to S3.", ValueType::MICROSECONDS) \
    M(WriteBufferFromS3Bytes, "Bytes written to S3.", ValueType::BYTES) \
    M(WriteBufferFromS3RequestsErrors, "Number of exceptions while writing to S3.", ValueType::NUMBER) \
    M(WriteBufferFromS3WaitInflightLimitMicroseconds, "Time spent on waiting while some of the current requests are done when its number reached the limit defined by s3_max_inflight_parts_for_one_file.", ValueType::MICROSECONDS) \
    M(QueryMemoryLimitExceeded, "Number of times when memory limit exceeded for query.", ValueType::NUMBER) \
    \
    M(AzureGetObject, "Number of Azure API GetObject calls.", ValueType::NUMBER) \
    M(AzureUploadPart, "Number of Azure blob storage API UploadPart calls", ValueType::NUMBER) \
    M(AzureCopyObject, "Number of Azure blob storage API CopyObject calls", ValueType::NUMBER) \
    M(AzureDeleteObjects, "Number of Azure blob storage API DeleteObject(s) calls.", ValueType::NUMBER) \
    M(AzureListObjects, "Number of Azure blob storage API ListObjects calls.", ValueType::NUMBER) \
    M(AzureGetProperties, "Number of Azure blob storage API GetProperties calls.", ValueType::NUMBER) \
    \
    M(DiskAzureGetObject, "Number of Disk Azure API GetObject calls.", ValueType::NUMBER) \
    M(DiskAzureUploadPart, "Number of Disk Azure blob storage API UploadPart calls", ValueType::NUMBER) \
    M(DiskAzureCopyObject, "Number of Disk Azure blob storage API CopyObject calls", ValueType::NUMBER) \
    M(DiskAzureListObjects, "Number of Disk Azure blob storage API ListObjects calls.", ValueType::NUMBER) \
    M(DiskAzureDeleteObjects, "Number of Azure blob storage API DeleteObject(s) calls.", ValueType::NUMBER) \
    M(DiskAzureGetProperties, "Number of Disk Azure blob storage API GetProperties calls.", ValueType::NUMBER) \
    \
    M(ReadBufferFromAzureMicroseconds, "Time spent on reading from Azure.", ValueType::MICROSECONDS) \
    M(ReadBufferFromAzureInitMicroseconds, "Time spent initializing connection to Azure.", ValueType::MICROSECONDS) \
    M(ReadBufferFromAzureBytes, "Bytes read from Azure.", ValueType::BYTES) \
    M(ReadBufferFromAzureRequestsErrors, "Number of exceptions while reading from Azure", ValueType::NUMBER) \
    \
    M(CachedReadBufferReadFromCacheHits, "Number of times the read from filesystem cache hit the cache.", ValueType::NUMBER) \
    M(CachedReadBufferReadFromCacheMisses, "Number of times the read from filesystem cache miss the cache.", ValueType::NUMBER) \
    M(CachedReadBufferReadFromSourceMicroseconds, "Time reading from filesystem cache source (from remote filesystem, etc)", ValueType::MICROSECONDS) \
    M(CachedReadBufferReadFromCacheMicroseconds, "Time reading from filesystem cache", ValueType::MICROSECONDS) \
    M(CachedReadBufferReadFromSourceBytes, "Bytes read from filesystem cache source (from remote fs, etc)", ValueType::BYTES) \
    M(CachedReadBufferReadFromCacheBytes, "Bytes read from filesystem cache", ValueType::BYTES) \
    M(CachedReadBufferCacheWriteBytes, "Bytes written from source (remote fs, etc) to filesystem cache", ValueType::BYTES) \
    M(CachedReadBufferCacheWriteMicroseconds, "Time spent writing data into filesystem cache", ValueType::MICROSECONDS) \
    M(CachedReadBufferCreateBufferMicroseconds, "Prepare buffer time", ValueType::MICROSECONDS) \
    M(CachedWriteBufferCacheWriteBytes, "Bytes written from source (remote fs, etc) to filesystem cache", ValueType::BYTES) \
    M(CachedWriteBufferCacheWriteMicroseconds, "Time spent writing data into filesystem cache", ValueType::MICROSECONDS) \
    \
    M(FilesystemCacheLoadMetadataMicroseconds, "Time spent loading filesystem cache metadata", ValueType::MICROSECONDS) \
    M(FilesystemCacheEvictedBytes, "Number of bytes evicted from filesystem cache", ValueType::BYTES) \
    M(FilesystemCacheEvictedFileSegments, "Number of file segments evicted from filesystem cache", ValueType::NUMBER) \
    M(FilesystemCacheEvictionSkippedFileSegments, "Number of file segments skipped for eviction because of being in unreleasable state", ValueType::NUMBER) \
    M(FilesystemCacheEvictionSkippedEvictingFileSegments, "Number of file segments skipped for eviction because of being in evicting state", ValueType::NUMBER) \
    M(FilesystemCacheEvictionTries, "Number of filesystem cache eviction attempts", ValueType::NUMBER) \
    M(FilesystemCacheLockKeyMicroseconds, "Lock cache key time", ValueType::MICROSECONDS) \
    M(FilesystemCacheLockMetadataMicroseconds, "Lock filesystem cache metadata time", ValueType::MICROSECONDS) \
    M(FilesystemCacheLockCacheMicroseconds, "Lock filesystem cache time", ValueType::MICROSECONDS) \
    M(FilesystemCacheReserveMicroseconds, "Filesystem cache space reservation time", ValueType::MICROSECONDS) \
    M(FilesystemCacheEvictMicroseconds, "Filesystem cache eviction time", ValueType::MICROSECONDS) \
    M(FilesystemCacheGetOrSetMicroseconds, "Filesystem cache getOrSet() time", ValueType::MICROSECONDS) \
    M(FilesystemCacheGetMicroseconds, "Filesystem cache get() time", ValueType::MICROSECONDS) \
    M(FileSegmentWaitMicroseconds, "Wait on DOWNLOADING state", ValueType::MICROSECONDS) \
    M(FileSegmentCompleteMicroseconds, "Duration of FileSegment::complete() in filesystem cache", ValueType::MICROSECONDS) \
    M(FileSegmentLockMicroseconds, "Lock file segment time", ValueType::MICROSECONDS) \
    M(FileSegmentWriteMicroseconds, "File segment write() time", ValueType::MICROSECONDS) \
    M(FileSegmentUseMicroseconds, "File segment use() time", ValueType::MICROSECONDS) \
    M(FileSegmentRemoveMicroseconds, "File segment remove() time", ValueType::MICROSECONDS) \
    M(FileSegmentHolderCompleteMicroseconds, "File segments holder complete() time", ValueType::MICROSECONDS) \
    M(FileSegmentFailToIncreasePriority, "Number of times the priority was not increased due to a high contention on the cache lock", ValueType::NUMBER) \
    M(FilesystemCacheFailToReserveSpaceBecauseOfLockContention, "Number of times space reservation was skipped due to a high contention on the cache lock", ValueType::NUMBER) \
    M(FilesystemCacheHoldFileSegments, "Filesystem cache file segments count, which were hold", ValueType::NUMBER) \
    M(FilesystemCacheUnusedHoldFileSegments, "Filesystem cache file segments count, which were hold, but not used (because of seek or LIMIT n, etc)", ValueType::NUMBER) \
    M(FilesystemCacheFreeSpaceKeepingThreadRun, "Number of times background thread executed free space keeping job", ValueType::NUMBER) \
    M(FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds, "Time for which background thread executed free space keeping job", ValueType::MILLISECONDS) \
    \
    M(RemoteFSSeeks, "Total number of seeks for async buffer", ValueType::NUMBER) \
    M(RemoteFSPrefetches, "Number of prefetches made with asynchronous reading from remote filesystem", ValueType::NUMBER) \
    M(RemoteFSCancelledPrefetches, "Number of cancelled prefecthes (because of seek)", ValueType::NUMBER) \
    M(RemoteFSUnusedPrefetches, "Number of prefetches pending at buffer destruction", ValueType::NUMBER) \
    M(RemoteFSPrefetchedReads, "Number of reads from prefecthed buffer", ValueType::NUMBER) \
    M(RemoteFSPrefetchedBytes, "Number of bytes from prefecthed buffer", ValueType::BYTES) \
    M(RemoteFSUnprefetchedReads, "Number of reads from unprefetched buffer", ValueType::NUMBER) \
    M(RemoteFSUnprefetchedBytes, "Number of bytes from unprefetched buffer", ValueType::BYTES) \
    M(RemoteFSLazySeeks, "Number of lazy seeks", ValueType::NUMBER) \
    M(RemoteFSSeeksWithReset, "Number of seeks which lead to a new connection", ValueType::NUMBER) \
    M(RemoteFSBuffers, "Number of buffers created for asynchronous reading from remote filesystem", ValueType::NUMBER) \
    M(MergeTreePrefetchedReadPoolInit, "Time spent preparing tasks in MergeTreePrefetchedReadPool", ValueType::MICROSECONDS) \
    M(WaitPrefetchTaskMicroseconds, "Time spend waiting for prefetched reader", ValueType::MICROSECONDS) \
    \
    M(ThreadpoolReaderTaskMicroseconds, "Time spent getting the data in asynchronous reading", ValueType::MICROSECONDS) \
    M(ThreadpoolReaderPrepareMicroseconds, "Time spent on preparation (e.g. call to reader seek() method)", ValueType::MICROSECONDS) \
    M(ThreadpoolReaderReadBytes, "Bytes read from a threadpool task in asynchronous reading", ValueType::BYTES) \
    M(ThreadpoolReaderSubmit, "Bytes read from a threadpool task in asynchronous reading", ValueType::BYTES) \
    M(ThreadpoolReaderSubmitReadSynchronously, "How many times we haven't scheduled a task on the thread pool and read synchronously instead", ValueType::NUMBER) \
    M(ThreadpoolReaderSubmitReadSynchronouslyBytes, "How many bytes were read synchronously", ValueType::BYTES) \
    M(ThreadpoolReaderSubmitReadSynchronouslyMicroseconds, "How much time we spent reading synchronously", ValueType::MICROSECONDS) \
    M(ThreadpoolReaderSubmitLookupInCacheMicroseconds, "How much time we spent checking if content is cached", ValueType::MICROSECONDS) \
    M(AsynchronousReaderIgnoredBytes, "Number of bytes ignored during asynchronous reading", ValueType::BYTES) \
    \
    M(FileSegmentWaitReadBufferMicroseconds, "Metric per file segment. Time spend waiting for internal read buffer (includes cache waiting)", ValueType::MICROSECONDS) \
    M(FileSegmentReadMicroseconds, "Metric per file segment. Time spend reading from file", ValueType::MICROSECONDS) \
    M(FileSegmentCacheWriteMicroseconds, "Metric per file segment. Time spend writing data to cache", ValueType::MICROSECONDS) \
    M(FileSegmentPredownloadMicroseconds, "Metric per file segment. Time spent pre-downloading data to cache (pre-downloading - finishing file segment download (after someone who failed to do that) up to the point current thread was requested to do)", ValueType::MICROSECONDS) \
    M(FileSegmentUsedBytes, "Metric per file segment. How many bytes were actually used from current file segment", ValueType::BYTES) \
    \
    M(ReadBufferSeekCancelConnection, "Number of seeks which lead to new connection (s3, http)", ValueType::NUMBER) \
    \
    M(SleepFunctionCalls, "Number of times a sleep function (sleep, sleepEachRow) has been called.", ValueType::NUMBER) \
    M(SleepFunctionMicroseconds, "Time set to sleep in a sleep function (sleep, sleepEachRow).", ValueType::MICROSECONDS) \
    M(SleepFunctionElapsedMicroseconds, "Time spent sleeping in a sleep function (sleep, sleepEachRow).", ValueType::MICROSECONDS) \
    \
    M(ThreadPoolReaderPageCacheHit, "Number of times the read inside ThreadPoolReader was done from the page cache.", ValueType::NUMBER) \
    M(ThreadPoolReaderPageCacheHitBytes, "Number of bytes read inside ThreadPoolReader when it was done from the page cache.", ValueType::BYTES) \
    M(ThreadPoolReaderPageCacheHitElapsedMicroseconds, "Time spent reading data from page cache in ThreadPoolReader.", ValueType::MICROSECONDS) \
    M(ThreadPoolReaderPageCacheMiss, "Number of times the read inside ThreadPoolReader was not done from page cache and was hand off to thread pool.", ValueType::NUMBER) \
    M(ThreadPoolReaderPageCacheMissBytes, "Number of bytes read inside ThreadPoolReader when read was not done from page cache and was hand off to thread pool.", ValueType::BYTES) \
    M(ThreadPoolReaderPageCacheMissElapsedMicroseconds, "Time spent reading data inside the asynchronous job in ThreadPoolReader - when read was not done from the page cache.", ValueType::MICROSECONDS) \
    \
    M(AsynchronousReadWaitMicroseconds, "Time spent in waiting for asynchronous reads in asynchronous local read.", ValueType::MICROSECONDS) \
    M(SynchronousReadWaitMicroseconds, "Time spent in waiting for synchronous reads in asynchronous local read.", ValueType::MICROSECONDS) \
    M(AsynchronousRemoteReadWaitMicroseconds, "Time spent in waiting for asynchronous remote reads.", ValueType::MICROSECONDS) \
    M(SynchronousRemoteReadWaitMicroseconds, "Time spent in waiting for synchronous remote reads.", ValueType::MICROSECONDS) \
    \
    M(ExternalDataSourceLocalCacheReadBytes, "Bytes read from local cache buffer in RemoteReadBufferCache", ValueType::BYTES) \
    \
    M(MainConfigLoads, "Number of times the main configuration was reloaded.", ValueType::NUMBER) \
    \
    M(AggregationPreallocatedElementsInHashTables, "How many elements were preallocated in hash tables for aggregation.", ValueType::NUMBER) \
    M(AggregationHashTablesInitializedAsTwoLevel, "How many hash tables were inited as two-level for aggregation.", ValueType::NUMBER) \
    M(AggregationOptimizedEqualRangesOfKeys, "For how many blocks optimization of equal ranges of keys was applied", ValueType::NUMBER) \
    \
    M(MetadataFromKeeperCacheHit, "Number of times an object storage metadata request was answered from cache without making request to Keeper", ValueType::NUMBER) \
    M(MetadataFromKeeperCacheMiss, "Number of times an object storage metadata request had to be answered from Keeper", ValueType::NUMBER) \
    M(MetadataFromKeeperCacheUpdateMicroseconds, "Total time spent in updating the cache including waiting for responses from Keeper", ValueType::MICROSECONDS) \
    M(MetadataFromKeeperUpdateCacheOneLevel, "Number of times a cache update for one level of directory tree was done", ValueType::NUMBER) \
    M(MetadataFromKeeperTransactionCommit, "Number of times metadata transaction commit was attempted", ValueType::NUMBER) \
    M(MetadataFromKeeperTransactionCommitRetry, "Number of times metadata transaction commit was retried", ValueType::NUMBER) \
    M(MetadataFromKeeperCleanupTransactionCommit, "Number of times metadata transaction commit for deleted objects cleanup was attempted", ValueType::NUMBER) \
    M(MetadataFromKeeperCleanupTransactionCommitRetry, "Number of times metadata transaction commit for deleted objects cleanup was retried", ValueType::NUMBER) \
    M(MetadataFromKeeperOperations, "Number of times a request was made to Keeper", ValueType::NUMBER) \
    M(MetadataFromKeeperIndividualOperations, "Number of paths read or written by single or multi requests to Keeper", ValueType::NUMBER) \
    M(MetadataFromKeeperReconnects, "Number of times a reconnect to Keeper was done", ValueType::NUMBER) \
    M(MetadataFromKeeperBackgroundCleanupObjects, "Number of times a old deleted object clean up was performed by background task", ValueType::NUMBER) \
    M(MetadataFromKeeperBackgroundCleanupTransactions, "Number of times old transaction idempotency token was cleaned up by background task", ValueType::NUMBER) \
    M(MetadataFromKeeperBackgroundCleanupErrors, "Number of times an error was encountered in background cleanup task", ValueType::NUMBER) \
    \
    M(KafkaRebalanceRevocations, "Number of partition revocations (the first stage of consumer group rebalance)", ValueType::NUMBER) \
    M(KafkaRebalanceAssignments, "Number of partition assignments (the final stage of consumer group rebalance)", ValueType::NUMBER) \
    M(KafkaRebalanceErrors, "Number of failed consumer group rebalances", ValueType::NUMBER) \
    M(KafkaMessagesPolled, "Number of Kafka messages polled from librdkafka to ClickHouse", ValueType::NUMBER) \
    M(KafkaMessagesRead, "Number of Kafka messages already processed by ClickHouse", ValueType::NUMBER) \
    M(KafkaMessagesFailed, "Number of Kafka messages ClickHouse failed to parse", ValueType::NUMBER) \
    M(KafkaRowsRead, "Number of rows parsed from Kafka messages", ValueType::NUMBER) \
    M(KafkaRowsRejected, "Number of parsed rows which were later rejected (due to rebalances / errors or similar reasons). Those rows will be consumed again after the rebalance.", ValueType::NUMBER) \
    M(KafkaDirectReads, "Number of direct selects from Kafka tables since server start", ValueType::NUMBER) \
    M(KafkaBackgroundReads, "Number of background reads populating materialized views from Kafka since server start", ValueType::NUMBER) \
    M(KafkaCommits, "Number of successful commits of consumed offsets to Kafka (normally should be the same as KafkaBackgroundReads)", ValueType::NUMBER) \
    M(KafkaCommitFailures, "Number of failed commits of consumed offsets to Kafka (usually is a sign of some data duplication)", ValueType::NUMBER) \
    M(KafkaConsumerErrors, "Number of errors reported by librdkafka during polls", ValueType::NUMBER) \
    M(KafkaWrites, "Number of writes (inserts) to Kafka tables ", ValueType::NUMBER) \
    M(KafkaRowsWritten, "Number of rows inserted into Kafka tables", ValueType::NUMBER) \
    M(KafkaProducerFlushes, "Number of explicit flushes to Kafka producer", ValueType::NUMBER) \
    M(KafkaMessagesProduced, "Number of messages produced to Kafka", ValueType::NUMBER) \
    M(KafkaProducerErrors, "Number of errors during producing the messages to Kafka", ValueType::NUMBER) \
    \
    M(ScalarSubqueriesGlobalCacheHit, "Number of times a read from a scalar subquery was done using the global cache", ValueType::NUMBER) \
    M(ScalarSubqueriesLocalCacheHit, "Number of times a read from a scalar subquery was done using the local cache", ValueType::NUMBER) \
    M(ScalarSubqueriesCacheMiss, "Number of times a read from a scalar subquery was not cached and had to be calculated completely", ValueType::NUMBER) \
    \
    M(SchemaInferenceCacheHits, "Number of times the requested source is found in schema cache", ValueType::NUMBER) \
    M(SchemaInferenceCacheSchemaHits, "Number of times the schema is found in schema cache during schema inference", ValueType::NUMBER) \
    M(SchemaInferenceCacheNumRowsHits, "Number of times the number of rows is found in schema cache during count from files", ValueType::NUMBER) \
    M(SchemaInferenceCacheMisses, "Number of times the requested source is not in schema cache", ValueType::NUMBER) \
    M(SchemaInferenceCacheSchemaMisses, "Number of times the requested source is in cache but the schema is not in cache during schema inference", ValueType::NUMBER) \
    M(SchemaInferenceCacheNumRowsMisses, "Number of times the requested source is in cache but the number of rows is not in cache while count from files", ValueType::NUMBER) \
    M(SchemaInferenceCacheEvictions, "Number of times a schema from cache was evicted due to overflow", ValueType::NUMBER) \
    M(SchemaInferenceCacheInvalidations, "Number of times a schema in cache became invalid due to changes in data", ValueType::NUMBER) \
    \
    M(KeeperPacketsSent, "Packets sent by keeper server", ValueType::NUMBER) \
    M(KeeperPacketsReceived, "Packets received by keeper server", ValueType::NUMBER) \
    M(KeeperRequestTotal, "Total requests number on keeper server", ValueType::NUMBER) \
    M(KeeperLatency, "Keeper latency", ValueType::MILLISECONDS) \
    M(KeeperCommits, "Number of successful commits", ValueType::NUMBER) \
    M(KeeperCommitsFailed, "Number of failed commits", ValueType::NUMBER) \
    M(KeeperSnapshotCreations, "Number of snapshots creations", ValueType::NUMBER) \
    M(KeeperSnapshotCreationsFailed, "Number of failed snapshot creations", ValueType::NUMBER) \
    M(KeeperSnapshotApplys, "Number of snapshot applying", ValueType::NUMBER) \
    M(KeeperSnapshotApplysFailed, "Number of failed snapshot applying", ValueType::NUMBER) \
    M(KeeperReadSnapshot, "Number of snapshot read(serialization)", ValueType::NUMBER) \
    M(KeeperSaveSnapshot, "Number of snapshot save", ValueType::NUMBER) \
    M(KeeperCreateRequest, "Number of create requests", ValueType::NUMBER) \
    M(KeeperRemoveRequest, "Number of remove requests", ValueType::NUMBER) \
    M(KeeperSetRequest, "Number of set requests", ValueType::NUMBER) \
    M(KeeperReconfigRequest, "Number of reconfig requests", ValueType::NUMBER) \
    M(KeeperCheckRequest, "Number of check requests", ValueType::NUMBER) \
    M(KeeperMultiRequest, "Number of multi requests", ValueType::NUMBER) \
    M(KeeperMultiReadRequest, "Number of multi read requests", ValueType::NUMBER) \
    M(KeeperGetRequest, "Number of get requests", ValueType::NUMBER) \
    M(KeeperListRequest, "Number of list requests", ValueType::NUMBER) \
    M(KeeperExistsRequest, "Number of exists requests", ValueType::NUMBER) \
    \
    M(OverflowBreak, "Number of times, data processing was cancelled by query complexity limitation with setting '*_overflow_mode' = 'break' and the result is incomplete.", ValueType::NUMBER) \
    M(OverflowThrow, "Number of times, data processing was cancelled by query complexity limitation with setting '*_overflow_mode' = 'throw' and exception was thrown.", ValueType::NUMBER) \
    M(OverflowAny, "Number of times approximate GROUP BY was in effect: when aggregation was performed only on top of first 'max_rows_to_group_by' unique keys and other keys were ignored due to 'group_by_overflow_mode' = 'any'.", ValueType::NUMBER) \
    \
    M(S3QueueSetFileProcessingMicroseconds, "Time spent to set file as processing", ValueType::MICROSECONDS) \
    M(S3QueueSetFileProcessedMicroseconds, "Time spent to set file as processed", ValueType::MICROSECONDS) \
    M(S3QueueSetFileFailedMicroseconds, "Time spent to set file as failed", ValueType::MICROSECONDS) \
    M(S3QueueCleanupMaxSetSizeOrTTLMicroseconds, "Time spent to set file as failed", ValueType::MICROSECONDS) \
    M(S3QueuePullMicroseconds, "Time spent to read file data", ValueType::MICROSECONDS) \
    M(S3QueueLockLocalFileStatusesMicroseconds, "Time spent to lock local file statuses", ValueType::MICROSECONDS) \
    \
    M(ServerStartupMilliseconds, "Time elapsed from starting server to listening to sockets in milliseconds", ValueType::MILLISECONDS) \
    M(IOUringSQEsSubmitted, "Total number of io_uring SQEs submitted", ValueType::NUMBER) \
    M(IOUringSQEsResubmits, "Total number of io_uring SQE resubmits performed", ValueType::NUMBER) \
    M(IOUringCQEsCompleted, "Total number of successfully completed io_uring CQEs", ValueType::NUMBER) \
    M(IOUringCQEsFailed, "Total number of completed io_uring CQEs with failures", ValueType::NUMBER) \
    \
    M(BackupsOpenedForRead, "Number of backups opened for reading", ValueType::NUMBER) \
    M(BackupsOpenedForWrite, "Number of backups opened for writing", ValueType::NUMBER) \
    M(BackupReadMetadataMicroseconds, "Time spent reading backup metadata from .backup file", ValueType::MICROSECONDS) \
    M(BackupWriteMetadataMicroseconds, "Time spent writing backup metadata to .backup file", ValueType::MICROSECONDS) \
    M(BackupEntriesCollectorMicroseconds, "Time spent making backup entries", ValueType::MICROSECONDS) \
    M(BackupEntriesCollectorForTablesDataMicroseconds, "Time spent making backup entries for tables data", ValueType::MICROSECONDS) \
    M(BackupEntriesCollectorRunPostTasksMicroseconds, "Time spent running post tasks after making backup entries", ValueType::MICROSECONDS) \
    \
    M(ReadTaskRequestsReceived, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the initiator server side.", ValueType::NUMBER) \
    M(MergeTreeReadTaskRequestsReceived, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the initiator server side.", ValueType::NUMBER) \
    \
    M(ReadTaskRequestsSent, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the remote server side.", ValueType::NUMBER) \
    M(MergeTreeReadTaskRequestsSent, "The number of callbacks requested from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the remote server side.", ValueType::NUMBER) \
    M(MergeTreeAllRangesAnnouncementsSent, "The number of announcements sent from the remote server to the initiator server about the set of data parts (for MergeTree tables). Measured on the remote server side.", ValueType::NUMBER) \
    M(ReadTaskRequestsSentElapsedMicroseconds, "Time spent in callbacks requested from the remote server back to the initiator server to choose the read task (for s3Cluster table function and similar). Measured on the remote server side.", ValueType::MICROSECONDS) \
    M(MergeTreeReadTaskRequestsSentElapsedMicroseconds, "Time spent in callbacks requested from the remote server back to the initiator server to choose the read task (for MergeTree tables). Measured on the remote server side.", ValueType::MICROSECONDS) \
    M(MergeTreeAllRangesAnnouncementsSentElapsedMicroseconds, "Time spent in sending the announcement from the remote server to the initiator server about the set of data parts (for MergeTree tables). Measured on the remote server side.", ValueType::MICROSECONDS) \
    \
    M(ConnectionPoolIsFullMicroseconds, "Total time spent waiting for a slot in connection pool.", ValueType::MICROSECONDS) \
    M(AsyncLoaderWaitMicroseconds, "Total time a query was waiting for async loader jobs.", ValueType::MICROSECONDS) \
    \
    M(DistrCacheServerSwitches, "Number of server switches between distributed cache servers in read/write-through cache", ValueType::NUMBER) \
    M(DistrCacheReadMicroseconds, "Time spent reading from distributed cache", ValueType::MICROSECONDS) \
    M(DistrCacheFallbackReadMicroseconds, "Time spend reading from fallback buffer instead of distribted cache", ValueType::MICROSECONDS) \
    M(DistrCachePrecomputeRangesMicroseconds, "Time spent to precompute read ranges", ValueType::MICROSECONDS) \
    M(DistrCacheNextImplMicroseconds, "Time spend in ReadBufferFromDistributedCache::nextImpl", ValueType::MICROSECONDS) \
    M(DistrCacheOpenedConnections, "The number of open connections to distributed cache", ValueType::NUMBER) \
    M(DistrCacheReusedConnections, "The number of reused connections to distributed cache", ValueType::NUMBER) \
    M(DistrCacheHoldConnections, "The number of used connections to distributed cache", ValueType::NUMBER) \
    \
    M(DistrCacheGetResponseMicroseconds, "Time spend to wait for response from distributed cache", ValueType::MICROSECONDS) \
    M(DistrCacheStartRangeMicroseconds, "Time spent to start a new read range with distributed cache", ValueType::MICROSECONDS) \
    M(DistrCacheLockRegistryMicroseconds, "Time spent to take DistributedCacheRegistry lock", ValueType::MICROSECONDS) \
    M(DistrCacheUnusedPackets, "Number of skipped unused packets from distributed cache", ValueType::NUMBER) \
    M(DistrCachePackets, "Total number of packets received from distributed cache", ValueType::NUMBER) \
    M(DistrCacheUnusedPacketsBytes, "The number of bytes in Data packets which were ignored", ValueType::BYTES) \
    M(DistrCacheRegistryUpdateMicroseconds, "Time spent updating distributed cache registry", ValueType::MICROSECONDS) \
    M(DistrCacheRegistryUpdates, "Number of distributed cache registry updates", ValueType::NUMBER) \
    \
    M(DistrCacheConnectMicroseconds, "The time spent to connect to distributed cache", ValueType::MICROSECONDS) \
    M(DistrCacheConnectAttempts, "The number of connection attempts to distributed cache") \
    M(DistrCacheGetClient, "Number of client access times", ValueType::NUMBER) \
    \
    M(DistrCacheServerProcessRequestMicroseconds, "Time spent processing request on DistributedCache server side", ValueType::MICROSECONDS) \
    \
    M(LogTest, "Number of log messages with level Test", ValueType::NUMBER) \
    M(LogTrace, "Number of log messages with level Trace", ValueType::NUMBER) \
    M(LogDebug, "Number of log messages with level Debug", ValueType::NUMBER) \
    M(LogInfo, "Number of log messages with level Info", ValueType::NUMBER) \
    M(LogWarning, "Number of log messages with level Warning", ValueType::NUMBER) \
    M(LogError, "Number of log messages with level Error", ValueType::NUMBER) \
    M(LogFatal, "Number of log messages with level Fatal", ValueType::NUMBER) \
    \
    M(InterfaceHTTPSendBytes, "Number of bytes sent through HTTP interfaces", ValueType::BYTES) \
    M(InterfaceHTTPReceiveBytes, "Number of bytes received through HTTP interfaces", ValueType::BYTES) \
    M(InterfaceNativeSendBytes, "Number of bytes sent through native interfaces", ValueType::BYTES) \
    M(InterfaceNativeReceiveBytes, "Number of bytes received through native interfaces", ValueType::BYTES) \
    M(InterfacePrometheusSendBytes, "Number of bytes sent through Prometheus interfaces", ValueType::BYTES) \
    M(InterfacePrometheusReceiveBytes, "Number of bytes received through Prometheus interfaces", ValueType::BYTES) \
    M(InterfaceInterserverSendBytes, "Number of bytes sent through interserver interfaces", ValueType::BYTES) \
    M(InterfaceInterserverReceiveBytes, "Number of bytes received through interserver interfaces", ValueType::BYTES) \
    M(InterfaceMySQLSendBytes, "Number of bytes sent through MySQL interfaces", ValueType::BYTES) \
    M(InterfaceMySQLReceiveBytes, "Number of bytes received through MySQL interfaces", ValueType::BYTES) \
    M(InterfacePostgreSQLSendBytes, "Number of bytes sent through PostgreSQL interfaces", ValueType::BYTES) \
    M(InterfacePostgreSQLReceiveBytes, "Number of bytes received through PostgreSQL interfaces", ValueType::BYTES) \
    \
    M(ParallelReplicasUsedCount, "Number of replicas used to execute a query with task-based parallel replicas", ValueType::NUMBER) \
    \
    M(KeeperLogsEntryReadFromLatestCache, "Number of log entries in Keeper being read from latest logs cache", ValueType::NUMBER) \
    M(KeeperLogsEntryReadFromCommitCache, "Number of log entries in Keeper being read from commit logs cache", ValueType::NUMBER) \
    M(KeeperLogsEntryReadFromFile, "Number of log entries in Keeper being read directly from the changelog file", ValueType::NUMBER) \
    M(KeeperLogsPrefetchedEntries, "Number of log entries in Keeper being prefetched from the changelog file", ValueType::NUMBER) \
    \
    M(ParallelReplicasAvailableCount, "Number of replicas available to execute a query with task-based parallel replicas", ValueType::NUMBER) \
    M(ParallelReplicasUnavailableCount, "Number of replicas which was chosen, but found to be unavailable during query execution with task-based parallel replicas", ValueType::NUMBER) \
    \
    M(StorageConnectionsCreated, "Number of created connections for storages", ValueType::NUMBER) \
    M(StorageConnectionsReused, "Number of reused connections for storages", ValueType::NUMBER) \
    M(StorageConnectionsReset, "Number of reset connections for storages", ValueType::NUMBER) \
    M(StorageConnectionsPreserved, "Number of preserved connections for storages", ValueType::NUMBER) \
    M(StorageConnectionsExpired, "Number of expired connections for storages", ValueType::NUMBER) \
    M(StorageConnectionsErrors, "Number of cases when creation of a connection for storage is failed", ValueType::NUMBER) \
    M(StorageConnectionsElapsedMicroseconds, "Total time spend on creating connections for storages", ValueType::MICROSECONDS)                                                                                                                                                                                                                                               \
    \
    M(DiskConnectionsCreated, "Number of created connections for disk", ValueType::NUMBER) \
    M(DiskConnectionsReused, "Number of reused connections for disk", ValueType::NUMBER) \
    M(DiskConnectionsReset, "Number of reset connections for disk", ValueType::NUMBER) \
    M(DiskConnectionsPreserved, "Number of preserved connections for disk", ValueType::NUMBER) \
    M(DiskConnectionsExpired, "Number of expired connections for disk", ValueType::NUMBER) \
    M(DiskConnectionsErrors, "Number of cases when creation of a connection for disk is failed", ValueType::NUMBER) \
    M(DiskConnectionsElapsedMicroseconds, "Total time spend on creating connections for disk", ValueType::MICROSECONDS) \
    \
    M(HTTPConnectionsCreated, "Number of created http connections", ValueType::NUMBER) \
    M(HTTPConnectionsReused, "Number of reused http connections", ValueType::NUMBER) \
    M(HTTPConnectionsReset, "Number of reset http connections", ValueType::NUMBER) \
    M(HTTPConnectionsPreserved, "Number of preserved http connections", ValueType::NUMBER) \
    M(HTTPConnectionsExpired, "Number of expired http connections", ValueType::NUMBER) \
    M(HTTPConnectionsErrors, "Number of cases when creation of a http connection failed", ValueType::NUMBER) \
    M(HTTPConnectionsElapsedMicroseconds, "Total time spend on creating http connections", ValueType::MICROSECONDS) \
    \
    M(AddressesDiscovered, "Total count of new addresses in dns resolve results for http connections", ValueType::NUMBER) \
    M(AddressesExpired, "Total count of expired addresses which is no longer presented in dns resolve results for http connections", ValueType::NUMBER) \
    M(AddressesMarkedAsFailed, "Total count of addresses which has been marked as faulty due to connection errors for http connections", ValueType::NUMBER) \
    \
    M(ReadWriteBufferFromHTTPRequestsSent, "Number of HTTP requests sent by ReadWriteBufferFromHTTP", ValueType::NUMBER) \
    M(ReadWriteBufferFromHTTPBytes, "Total size of payload bytes received and sent by ReadWriteBufferFromHTTP. Doesn't include HTTP headers.", ValueType::BYTES) \


#ifdef APPLY_FOR_EXTERNAL_EVENTS
    #define APPLY_FOR_EVENTS(M) APPLY_FOR_BUILTIN_EVENTS(M) APPLY_FOR_EXTERNAL_EVENTS(M)
#else
    #define APPLY_FOR_EVENTS(M) APPLY_FOR_BUILTIN_EVENTS(M)
#endif

namespace ProfileEvents
{

#define M(NAME, DOCUMENTATION, VALUE_TYPE) extern const Event NAME = Event(__COUNTER__);
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
