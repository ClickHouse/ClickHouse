#include <IO/ReaderExecutorStats.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event ReaderExecutorBytesFromPageCache;
    extern const Event ReaderExecutorBytesFromFilesystemCache;
    extern const Event ReaderExecutorBytesFromSource;
    extern const Event ReaderExecutorBytesPushedToCacheSync;
    extern const Event ReaderExecutorCacheGetRequests;
    extern const Event ReaderExecutorCachePopulateRequests;
    extern const Event ReaderExecutorSourceRequests;
    extern const Event ReaderExecutorIncompleteConnections;
    extern const Event ReaderExecutorModeledCostMicroseconds;
    extern const Event ReaderExecutorDeliveredBytes;
    extern const Event ReaderExecutorCacheGetMicroseconds;
    extern const Event ReaderExecutorCachePopulateMicroseconds;
    extern const Event ReaderExecutorSourceReadMicroseconds;
    extern const Event ReaderExecutorDecryptMicroseconds;
    extern const Event ReaderExecutorPrefetchWaitMicroseconds;
    extern const Event ReaderExecutorWorkMicroseconds;
    extern const Event ReaderExecutorPrefetchHits;
    extern const Event ReaderExecutorPrefetchCancelled;
    extern const Event ReaderExecutorPrefetchPoolFull;
    extern const Event ReaderExecutorPrefetchDiscardedRunning;
    extern const Event ReaderExecutorPrefetchIssuedSourceBytes;
    extern const Event ReaderExecutorPrefetchWastedSourceBytes;
    extern const Event ReaderExecutorMachineInterrupted;
    extern const Event ReaderExecutorPartialCollects;
    extern const Event ReaderExecutorPutFailed;
    extern const Event ReaderExecutorLongConnectionOpened;
    extern const Event ReaderExecutorLongConnectionHits;
    extern const Event ReaderExecutorLongConnectionFallbacks;
    extern const Event ReaderExecutorLongConnectionBytes;
    extern const Event ReaderExecutorObservations;
    extern const Event ReaderExecutorPlanExtensions;
}

namespace DB
{

/// The ONE place a counter is mapped to its ProfileEvent. Bump the counter, emit the event,
/// and (for the cost-model counters) add the modeled-cost contribution - so a running query's
/// events advance as the read happens. The prefetch worker runs in the submitter's thread
/// group (attached by `PrefetchThreadPool`), so a worker-thread emit attributes to the query.
/// The bytes term's per-increment integer rounding is negligible against the millisecond model.
void ReaderExecutorStats::add(Counter c, UInt64 value)
{
    values[c] += value;
    switch (c)
    {
        case BytesFromPageCache:        ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesFromPageCache, value); break;
        case BytesFromFilesystemCache:  ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesFromFilesystemCache, value); break;
        case BytesFromSource:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesFromSource, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 20000ULL * value / (1024 * 1024));
            break;
        case BytesPushedToCacheSync:    ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesPushedToCacheSync, value); break;
        case CacheGetRequests:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorCacheGetRequests, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 50 * value);
            break;
        case CachePopulateRequests:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorCachePopulateRequests, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 100 * value);
            break;
        case SourceRequests:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorSourceRequests, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 30000 * value);
            break;
        case IncompleteConnections:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorIncompleteConnections, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 5000 * value);
            break;
        case DeliveredBytes:            ProfileEvents::increment(ProfileEvents::ReaderExecutorDeliveredBytes, value); break;
        case CacheGetMicroseconds:      ProfileEvents::increment(ProfileEvents::ReaderExecutorCacheGetMicroseconds, value); break;
        case CachePopulateMicroseconds: ProfileEvents::increment(ProfileEvents::ReaderExecutorCachePopulateMicroseconds, value); break;
        case SourceReadMicroseconds:    ProfileEvents::increment(ProfileEvents::ReaderExecutorSourceReadMicroseconds, value); break;
        case DecryptMicroseconds:       ProfileEvents::increment(ProfileEvents::ReaderExecutorDecryptMicroseconds, value); break;
        case PrefetchWaitMicroseconds:  ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchWaitMicroseconds, value); break;
        case WorkMicroseconds:          ProfileEvents::increment(ProfileEvents::ReaderExecutorWorkMicroseconds, value); break;
        case PrefetchHits:              ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchHits, value); break;
        case PrefetchCancelled:         ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchCancelled, value); break;
        case PrefetchPoolFull:          ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchPoolFull, value); break;
        case PrefetchDiscardedRunning:  ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchDiscardedRunning, value); break;
        case PrefetchIssuedSourceBytes: ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchIssuedSourceBytes, value); break;
        case PrefetchWastedSourceBytes: ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchWastedSourceBytes, value); break;
        case MachineInterrupted:        ProfileEvents::increment(ProfileEvents::ReaderExecutorMachineInterrupted, value); break;
        case PartialCollects:           ProfileEvents::increment(ProfileEvents::ReaderExecutorPartialCollects, value); break;
        case PutFailed:                 ProfileEvents::increment(ProfileEvents::ReaderExecutorPutFailed, value); break;
        case LongConnectionOpened:      ProfileEvents::increment(ProfileEvents::ReaderExecutorLongConnectionOpened, value); break;
        case LongConnectionHits:        ProfileEvents::increment(ProfileEvents::ReaderExecutorLongConnectionHits, value); break;
        case LongConnectionFallbacks:   ProfileEvents::increment(ProfileEvents::ReaderExecutorLongConnectionFallbacks, value); break;
        case LongConnectionBytes:       ProfileEvents::increment(ProfileEvents::ReaderExecutorLongConnectionBytes, value); break;
        case Observations:              ProfileEvents::increment(ProfileEvents::ReaderExecutorObservations, value); break;
        case PlanExtensions:            ProfileEvents::increment(ProfileEvents::ReaderExecutorPlanExtensions, value); break;
        case NumCounters:               break;
    }
}


ReaderExecutorStats & ReaderExecutorStats::operator+=(const ReaderExecutorStats & o)
{
    for (size_t i = 0; i < NumCounters; ++i)
        values[i] += o.values[i];
    return *this;
}

ReaderExecutorStatTimer::ReaderExecutorStatTimer(ReaderExecutorStats & stats_, ReaderExecutorStats::Counter counter_)
    : target(stats_)
    , counter(counter_)
{
}

ReaderExecutorStatTimer::~ReaderExecutorStatTimer()
{
    target.add(counter, watch.elapsedMicroseconds());
}

}
