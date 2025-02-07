#include <atomic>
#include <Common/logger_useful.h>
#include <Common/LatencyBuckets.h>

#define APPLY_FOR_EVENTS(M) \
    M(S3FirstByteReadAttempt1Microseconds, "Time of first byte read from S3 storage (attempt 1).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(S3FirstByteWriteAttempt1Microseconds, "Time of first byte write to S3 storage (attempt 1).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(S3FirstByteReadAttempt2Microseconds, "Time of first byte read from S3 storage (attempt 2).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(S3FirstByteWriteAttempt2Microseconds, "Time of first byte write to S3 storage (attempt 2).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(S3FirstByteReadAttemptNMicroseconds, "Time of first byte read from S3 storage (attempt N).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(S3FirstByteWriteAttemptNMicroseconds, "Time of first byte write to S3 storage (attempt N).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(S3ConnectMicroseconds, "Time to connect for requests to S3 storage.", 100, 1000, 10000, 100000, 200000, 300000, 500000, 1000000, 1500000) \
    M(DiskS3FirstByteReadAttempt1Microseconds, "Time of first byte read from DiskS3 storage (attempt 1).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(DiskS3FirstByteWriteAttempt1Microseconds, "Time of first byte write to DiskS3 storage (attempt 1).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(DiskS3FirstByteReadAttempt2Microseconds, "Time of first byte read from DiskS3 storage (attempt 2).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(DiskS3FirstByteWriteAttempt2Microseconds, "Time of first byte write to DiskS3 storage (attempt 2).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(DiskS3FirstByteReadAttemptNMicroseconds, "Time of first byte read from DiskS3 storage (attempt N).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(DiskS3FirstByteWriteAttemptNMicroseconds, "Time of first byte write to DiskS3 storage (attempt N).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(DiskS3ConnectMicroseconds, "Time to connect for requests to DiskS3 storage.", 100, 1000, 10000, 100000, 200000, 300000, 500000, 1000000, 1500000) \


namespace LatencyBuckets
{

#define M(NAME, DOCUMENTATION, ...) extern const Event NAME = Event(__COUNTER__);
    APPLY_FOR_EVENTS(M)
#undef M

constexpr Event END = Event(__COUNTER__);

BucketList global_bucket_lists_array[END] =
{
#define M(NAME, DOCUMENTATION, ...) BucketList(CH_VA_ARGS_NARGS(__VA_ARGS__) + 1),
    APPLY_FOR_EVENTS(M)
#undef M
};
BucketLists global_bucket_lists(global_bucket_lists_array);

const Event BucketLists::num_events = END;

void BucketLists::reset()
{
    if (bucket_lists)
    {
        for (Event i = Event(0); i < num_events; ++i)
            for (auto & bucket : bucket_lists[i])
                bucket.store(0, std::memory_order_relaxed);
    }
}

const char * getName(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, ...) #NAME,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

const char * getDocumentation(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION, ...) DOCUMENTATION "",
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

std::vector<Count> & getBucketBounds(Event event)
{
    static std::vector<Count> all_buckets_bounds[] =
    {
    #define M(NAME, DOCUMENTATION, ...) {__VA_ARGS__, UINT_MAX},
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return all_buckets_bounds[event];
}

Event end() { return END; }

void increment(Event event, Count amount)
{
    global_bucket_lists.increment(event, amount);
}

void BucketLists::increment(Event event, Count amount)
{
    auto & bucket_bounds = getBucketBounds(event);
    auto & bucket_list = this->bucket_lists[event];
    auto ind = std::lower_bound(bucket_bounds.begin(), bucket_bounds.end(), amount) - bucket_bounds.begin();
    bucket_list[ind].fetch_add(1, std::memory_order_relaxed);
}

}

#undef APPLY_FOR_EVENTS
