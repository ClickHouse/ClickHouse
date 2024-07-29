#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

/// Events which are useful for Keeper.
/// New events should be added manually.
#define APPLY_FOR_KEEPER_PROFILE_EVENTS(M) \
    M(FileOpen) \
    M(Seek) \
    M(ReadBufferFromFileDescriptorRead) \
    M(ReadBufferFromFileDescriptorReadFailed) \
    M(ReadBufferFromFileDescriptorReadBytes) \
    M(WriteBufferFromFileDescriptorWrite) \
    M(WriteBufferFromFileDescriptorWriteFailed) \
    M(WriteBufferFromFileDescriptorWriteBytes) \
    M(FileSync) \
    M(DirectorySync) \
    M(FileSyncElapsedMicroseconds) \
    M(DirectorySyncElapsedMicroseconds) \
    M(ReadCompressedBytes) \
    M(CompressedReadBufferBlocks) \
    M(CompressedReadBufferBytes) \
    M(AIOWrite) \
    M(AIOWriteBytes) \
    M(AIORead) \
    M(AIOReadBytes) \
    M(IOBufferAllocs) \
    M(IOBufferAllocBytes) \
    M(ArenaAllocChunks) \
    M(ArenaAllocBytes) \
    M(CreatedReadBufferOrdinary) \
    M(CreatedReadBufferDirectIO) \
    M(CreatedReadBufferDirectIOFailed) \
    M(CreatedReadBufferMMap) \
    M(CreatedReadBufferMMapFailed) \
    M(DiskReadElapsedMicroseconds) \
    M(DiskWriteElapsedMicroseconds) \
    M(NetworkReceiveElapsedMicroseconds) \
    M(NetworkSendElapsedMicroseconds) \
    M(NetworkReceiveBytes) \
    M(NetworkSendBytes) \
\
    M(DiskS3GetRequestThrottlerCount) \
    M(DiskS3GetRequestThrottlerSleepMicroseconds) \
    M(DiskS3PutRequestThrottlerCount) \
    M(DiskS3PutRequestThrottlerSleepMicroseconds) \
    M(S3GetRequestThrottlerCount) \
    M(S3GetRequestThrottlerSleepMicroseconds) \
    M(S3PutRequestThrottlerCount) \
    M(S3PutRequestThrottlerSleepMicroseconds) \
    M(RemoteReadThrottlerBytes) \
    M(RemoteReadThrottlerSleepMicroseconds) \
    M(RemoteWriteThrottlerBytes) \
    M(RemoteWriteThrottlerSleepMicroseconds) \
    M(LocalReadThrottlerBytes) \
    M(LocalReadThrottlerSleepMicroseconds) \
    M(LocalWriteThrottlerBytes) \
    M(LocalWriteThrottlerSleepMicroseconds) \
    M(ThrottlerSleepMicroseconds) \
\
    M(SlowRead) \
    M(ReadBackoff) \
\
    M(ContextLock) \
    M(ContextLockWaitMicroseconds) \
\
    M(RWLockAcquiredReadLocks) \
    M(RWLockAcquiredWriteLocks) \
    M(RWLockReadersWaitMilliseconds) \
    M(RWLockWritersWaitMilliseconds) \
    M(DNSError) \
    M(RealTimeMicroseconds) \
    M(UserTimeMicroseconds) \
    M(SystemTimeMicroseconds) \
    M(MemoryOvercommitWaitTimeMicroseconds) \
    M(MemoryAllocatorPurge) \
    M(MemoryAllocatorPurgeTimeMicroseconds) \
    M(SoftPageFaults) \
    M(HardPageFaults) \
\
    M(OSIOWaitMicroseconds) \
    M(OSCPUWaitMicroseconds) \
    M(OSCPUVirtualTimeMicroseconds) \
    M(OSReadBytes) \
    M(OSWriteBytes) \
    M(OSReadChars) \
    M(OSWriteChars) \
\
    M(PerfCPUCycles) \
    M(PerfInstructions) \
    M(PerfCacheReferences) \
    M(PerfCacheMisses) \
    M(PerfBranchInstructions) \
    M(PerfBranchMisses) \
    M(PerfBusCycles) \
    M(PerfStalledCyclesFrontend) \
    M(PerfStalledCyclesBackend) \
    M(PerfRefCPUCycles) \
\
    M(PerfCPUClock) \
    M(PerfTaskClock) \
    M(PerfContextSwitches) \
    M(PerfCPUMigrations) \
    M(PerfAlignmentFaults) \
    M(PerfEmulationFaults) \
    M(PerfMinEnabledTime) \
    M(PerfMinEnabledRunningTime) \
    M(PerfDataTLBReferences) \
    M(PerfDataTLBMisses) \
    M(PerfInstructionTLBReferences) \
    M(PerfInstructionTLBMisses) \
    M(PerfLocalMemoryReferences) \
    M(PerfLocalMemoryMisses) \
\
    M(CannotWriteToWriteBufferDiscard) \
\
    M(S3ReadMicroseconds) \
    M(S3ReadRequestsCount) \
    M(S3ReadRequestsErrors) \
    M(S3ReadRequestsThrottling) \
    M(S3ReadRequestsRedirects) \
\
    M(S3WriteMicroseconds) \
    M(S3WriteRequestsCount) \
    M(S3WriteRequestsErrors) \
    M(S3WriteRequestsThrottling) \
    M(S3WriteRequestsRedirects) \
\
    M(DiskS3ReadMicroseconds) \
    M(DiskS3ReadRequestsCount) \
    M(DiskS3ReadRequestsErrors) \
    M(DiskS3ReadRequestsThrottling) \
    M(DiskS3ReadRequestsRedirects) \
\
    M(DiskS3WriteMicroseconds) \
    M(DiskS3WriteRequestsCount) \
    M(DiskS3WriteRequestsErrors) \
    M(DiskS3WriteRequestsThrottling) \
    M(DiskS3WriteRequestsRedirects) \
\
    M(S3DeleteObjects) \
    M(S3CopyObject) \
    M(S3ListObjects) \
    M(S3HeadObject) \
    M(S3GetObjectAttributes) \
    M(S3CreateMultipartUpload) \
    M(S3UploadPartCopy) \
    M(S3UploadPart) \
    M(S3AbortMultipartUpload) \
    M(S3CompleteMultipartUpload) \
    M(S3PutObject) \
    M(S3GetObject) \
\
    M(AzureUpload) \
    M(DiskAzureUpload) \
    M(AzureStageBlock) \
    M(DiskAzureStageBlock) \
    M(AzureCommitBlockList) \
    M(DiskAzureCommitBlockList) \
    M(AzureCopyObject) \
    M(DiskAzureCopyObject) \
    M(AzureDeleteObjects) \
    M(DiskAzureDeleteObjects) \
    M(AzureListObjects) \
    M(DiskAzureListObjects) \
\
    M(DiskS3DeleteObjects) \
    M(DiskS3CopyObject) \
    M(DiskS3ListObjects) \
    M(DiskS3HeadObject) \
    M(DiskS3GetObjectAttributes) \
    M(DiskS3CreateMultipartUpload) \
    M(DiskS3UploadPartCopy) \
    M(DiskS3UploadPart) \
    M(DiskS3AbortMultipartUpload) \
    M(DiskS3CompleteMultipartUpload) \
    M(DiskS3PutObject) \
    M(DiskS3GetObject) \
\
    M(S3Clients) \
    M(TinyS3Clients) \
\
    M(ReadBufferFromS3Microseconds) \
    M(ReadBufferFromS3InitMicroseconds) \
    M(ReadBufferFromS3Bytes) \
    M(ReadBufferFromS3RequestsErrors) \
\
    M(WriteBufferFromS3Microseconds) \
    M(WriteBufferFromS3Bytes) \
    M(WriteBufferFromS3RequestsErrors) \
    M(WriteBufferFromS3WaitInflightLimitMicroseconds) \
    M(RemoteFSSeeks) \
    M(RemoteFSPrefetches) \
    M(RemoteFSCancelledPrefetches) \
    M(RemoteFSUnusedPrefetches) \
    M(RemoteFSPrefetchedReads) \
    M(RemoteFSPrefetchedBytes) \
    M(RemoteFSUnprefetchedReads) \
    M(RemoteFSUnprefetchedBytes) \
    M(RemoteFSLazySeeks) \
    M(RemoteFSSeeksWithReset) \
    M(RemoteFSBuffers) \
\
    M(ThreadpoolReaderTaskMicroseconds) \
    M(ThreadpoolReaderPrepareMicroseconds) \
    M(ThreadpoolReaderReadBytes) \
    M(ThreadpoolReaderSubmit) \
    M(ThreadpoolReaderSubmitReadSynchronously) \
    M(ThreadpoolReaderSubmitReadSynchronouslyBytes) \
    M(ThreadpoolReaderSubmitReadSynchronouslyMicroseconds) \
    M(ThreadpoolReaderSubmitLookupInCacheMicroseconds) \
    M(AsynchronousReaderIgnoredBytes) \
\
    M(FileSegmentWaitReadBufferMicroseconds) \
    M(FileSegmentReadMicroseconds) \
    M(FileSegmentCacheWriteMicroseconds) \
    M(FileSegmentPredownloadMicroseconds) \
    M(FileSegmentUsedBytes) \
\
    M(ReadBufferSeekCancelConnection) \
\
    M(SleepFunctionCalls) \
    M(SleepFunctionMicroseconds) \
    M(SleepFunctionElapsedMicroseconds) \
\
    M(ThreadPoolReaderPageCacheHit) \
    M(ThreadPoolReaderPageCacheHitBytes) \
    M(ThreadPoolReaderPageCacheHitElapsedMicroseconds) \
    M(ThreadPoolReaderPageCacheMiss) \
    M(ThreadPoolReaderPageCacheMissBytes) \
    M(ThreadPoolReaderPageCacheMissElapsedMicroseconds) \
\
    M(AsynchronousReadWaitMicroseconds) \
    M(SynchronousReadWaitMicroseconds) \
    M(AsynchronousRemoteReadWaitMicroseconds) \
    M(SynchronousRemoteReadWaitMicroseconds) \
\
    M(ExternalDataSourceLocalCacheReadBytes) \
\
    M(MainConfigLoads) \
\
    M(KeeperPacketsSent) \
    M(KeeperPacketsReceived) \
    M(KeeperRequestTotal) \
    M(KeeperLatency) \
    M(KeeperTotalElapsedMicroseconds) \
    M(KeeperProcessElapsedMicroseconds) \
    M(KeeperPreprocessElapsedMicroseconds) \
    M(KeeperStorageLockWaitMicroseconds) \
    M(KeeperCommitWaitElapsedMicroseconds) \
    M(KeeperBatchMaxCount) \
    M(KeeperBatchMaxTotalSize) \
    M(KeeperCommits) \
    M(KeeperCommitsFailed) \
    M(KeeperSnapshotCreations) \
    M(KeeperSnapshotCreationsFailed) \
    M(KeeperSnapshotApplys) \
    M(KeeperSnapshotApplysFailed) \
    M(KeeperReadSnapshot) \
    M(KeeperSaveSnapshot) \
    M(KeeperCreateRequest) \
    M(KeeperRemoveRequest) \
    M(KeeperSetRequest) \
    M(KeeperReconfigRequest) \
    M(KeeperCheckRequest) \
    M(KeeperMultiRequest) \
    M(KeeperMultiReadRequest) \
    M(KeeperGetRequest) \
    M(KeeperListRequest) \
    M(KeeperExistsRequest) \
\
    M(IOUringSQEsSubmitted) \
    M(IOUringSQEsResubmitsAsync) \
    M(IOUringSQEsResubmitsSync) \
    M(IOUringCQEsCompleted) \
    M(IOUringCQEsFailed) \
\
    M(LogTest) \
    M(LogTrace) \
    M(LogDebug) \
    M(LogInfo) \
    M(LogWarning) \
    M(LogError) \
    M(LogFatal) \
\
    M(InterfaceHTTPSendBytes) \
    M(InterfaceHTTPReceiveBytes) \
    M(InterfaceNativeSendBytes) \
    M(InterfaceNativeReceiveBytes) \
    M(InterfacePrometheusSendBytes) \
    M(InterfacePrometheusReceiveBytes) \
    M(InterfaceInterserverSendBytes) \
    M(InterfaceInterserverReceiveBytes) \
    M(InterfaceMySQLSendBytes) \
    M(InterfaceMySQLReceiveBytes) \
    M(InterfacePostgreSQLSendBytes) \
    M(InterfacePostgreSQLReceiveBytes) \
\
    M(KeeperLogsEntryReadFromLatestCache) \
    M(KeeperLogsEntryReadFromCommitCache) \
    M(KeeperLogsEntryReadFromFile) \
    M(KeeperLogsPrefetchedEntries) \

namespace ProfileEvents
{
#define M(NAME) extern const Event NAME;
    APPLY_FOR_KEEPER_PROFILE_EVENTS(M)
#undef M

#define M(NAME) NAME,
extern const std::vector<Event> keeper_profile_events
{
    APPLY_FOR_KEEPER_PROFILE_EVENTS(M)
};
#undef M
}

/// Metrics which are useful for Keeper.
/// New metrics should be added manually.
#define APPLY_FOR_KEEPER_METRICS(M) \
    M(BackgroundCommonPoolTask) \
    M(BackgroundCommonPoolSize) \
    M(TCPConnection) \
    M(HTTPConnection) \
    M(OpenFileForRead) \
    M(OpenFileForWrite) \
    M(Read) \
    M(RemoteRead) \
    M(Write) \
    M(NetworkReceive) \
    M(NetworkSend) \
    M(MemoryTracking) \
    M(ContextLockWait) \
    M(Revision) \
    M(VersionInteger) \
    M(RWLockWaitingReaders) \
    M(RWLockWaitingWriters) \
    M(RWLockActiveReaders) \
    M(RWLockActiveWriters) \
    M(GlobalThread) \
    M(GlobalThreadActive) \
    M(GlobalThreadScheduled) \
    M(LocalThread) \
    M(LocalThreadActive) \
    M(LocalThreadScheduled) \
    M(IOPrefetchThreads) \
    M(IOPrefetchThreadsActive) \
    M(IOPrefetchThreadsScheduled) \
    M(IOWriterThreads) \
    M(IOWriterThreadsActive) \
    M(IOWriterThreadsScheduled) \
    M(IOThreads) \
    M(IOThreadsActive) \
    M(IOThreadsScheduled) \
    M(ThreadPoolRemoteFSReaderThreads) \
    M(ThreadPoolRemoteFSReaderThreadsActive) \
    M(ThreadPoolRemoteFSReaderThreadsScheduled) \
    M(ThreadPoolFSReaderThreads) \
    M(ThreadPoolFSReaderThreadsActive) \
    M(ThreadPoolFSReaderThreadsScheduled) \
    M(DiskObjectStorageAsyncThreads) \
    M(DiskObjectStorageAsyncThreadsActive) \
    M(ObjectStorageS3Threads) \
    M(ObjectStorageS3ThreadsActive) \
    M(ObjectStorageS3ThreadsScheduled) \
    M(ObjectStorageAzureThreads) \
    M(ObjectStorageAzureThreadsActive) \
    M(ObjectStorageAzureThreadsScheduled) \
    M(MMappedFiles) \
    M(MMappedFileBytes) \
    M(AsynchronousReadWait) \
    M(S3Requests) \
    M(KeeperAliveConnections) \
    M(KeeperOutstandingRequests) \
    M(ThreadsInOvercommitTracker) \
    M(IOUringPendingEvents) \
    M(IOUringInFlightEvents) \

namespace CurrentMetrics
{
#define M(NAME) extern const Metric NAME;
    APPLY_FOR_KEEPER_METRICS(M)
#undef M

#define M(NAME) NAME,
extern const std::vector<Metric> keeper_metrics
{
    APPLY_FOR_KEEPER_METRICS(M)
};
#undef M
}
