#pragma once

/// Initial audited nonallocating events; legacy dynamic/destructor publishers still require an audit.
/// Standalone layouts reserve this subset; it does not authorize paged process-counter storage.
#define APPLY_FOR_NON_ALLOCATING_PROFILE_EVENTS(M) \
    M(QueryProfilerConcurrencyOverruns) \
    M(QueryProfilerSignalOverruns) \
    M(QueryProfilerErrors) \
    M(QueryProfilerRuns) \
    M(CannotWriteToWriteBufferDiscard) \
    M(MemoryAllocatedWithoutCheck) \
    M(MemoryAllocatedWithoutCheckBytes) \
    M(QueryMemoryLimitExceeded) \
    M(GlobalMemoryLimitExceeded) \
    M(PageCacheOvercommitResize) \
    M(ConcurrencyControlWaitMicroseconds) \
    M(ConcurrencyControlPreemptedMicroseconds) \
    M(ConcurrencyControlSlotsAcquired) \
    M(ConcurrencyControlSlotsAcquiredNonCompeting) \
    M(ConcurrencyControlUpscales) \
    M(ConcurrencyControlDownscales) \
    M(ConcurrencyControlPreemptions) \
    M(MemoryReservationAdmitMicroseconds) \
    M(MemoryReservationIncreaseMicroseconds) \
    M(MemoryReservationIncreases) \
    M(MemoryReservationDecreases) \
    M(MemoryReservationFailed) \
    M(MemoryReservationKilled) \
    M(SchedulerIOReadRequests) \
    M(SchedulerIOReadBytes) \
    M(SchedulerIOReadWaitMicroseconds) \
    M(SchedulerIOWriteRequests) \
    M(SchedulerIOWriteBytes) \
    M(SchedulerIOWriteWaitMicroseconds) \
    M(BackupEntriesCollectorForTablesDataMicroseconds) \
    M(BackupEntriesCollectorMicroseconds) \
    M(BackupEntriesCollectorRunPostTasksMicroseconds) \
    M(BackupPreparingFileInfosMicroseconds) \
    M(BackupReadMetadataMicroseconds) \
    M(BackupWriteMetadataMicroseconds) \
    M(ConcurrentQueryWaitMicroseconds) \
    M(FilesystemCacheEvictMicroseconds) \
    M(ObjectStorageQueueCleanupMaxSetSizeOrTTLMicroseconds) \
    M(ObjectStorageQueuePullMicroseconds) \
    M(ThrottlerSleepMicroseconds)
