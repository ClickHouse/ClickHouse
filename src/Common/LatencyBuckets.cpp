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
    M(AzureFirstByteReadAttempt1Microseconds, "Time of first byte read from Azure Blob Storage (attempt 1).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(AzureFirstByteWriteAttempt1Microseconds, "Time of first byte write to Azure Blob Storage (attempt 1).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(AzureFirstByteReadAttempt2Microseconds, "Time of first byte read from Azure Blob Storage (attempt 2).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(AzureFirstByteWriteAttempt2Microseconds, "Time of first byte write to Azure Blob Storage (attempt 2).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(AzureFirstByteReadAttemptNMicroseconds, "Time of first byte read from Azure Blob Storage (attempt N).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(AzureFirstByteWriteAttemptNMicroseconds, "Time of first byte write to Azure Blob Storage (attempt N).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(AzureConnectMicroseconds, "Time to connect for requests to Azure Blob Storage.", 100, 1000, 10000, 100000, 200000, 300000, 500000, 1000000, 1500000) \
    M(DiskAzureFirstByteReadAttempt1Microseconds, "Time of first byte read from DiskAzure storage (attempt 1).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(DiskAzureFirstByteWriteAttempt1Microseconds, "Time of first byte write to DiskAzure storage (attempt 1).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(DiskAzureFirstByteReadAttempt2Microseconds, "Time of first byte read from DiskAzure storage (attempt 2).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(DiskAzureFirstByteWriteAttempt2Microseconds, "Time of first byte write to DiskAzure storage (attempt 2).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(DiskAzureFirstByteReadAttemptNMicroseconds, "Time of first byte read from DiskAzure storage (attempt N).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(DiskAzureFirstByteWriteAttemptNMicroseconds, "Time of first byte write to DiskAzure storage (attempt N).", 100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000) \
    M(DiskAzureConnectMicroseconds, "Time to connect for requests to DiskAzure storage.", 100, 1000, 10000, 100000, 200000, 300000, 500000, 1000000, 1500000) \


namespace LatencyBuckets
{

#define M(NAME, DOCUMENTATION, ...) extern const LatencyEvent NAME = LatencyEvent(__COUNTER__);
    APPLY_FOR_EVENTS(M)
#undef M

constexpr LatencyEvent END = LatencyEvent(__COUNTER__);

BucketList global_bucket_lists_array[END] =
{
#define M(NAME, DOCUMENTATION, ...) BucketList(CH_VA_ARGS_NARGS(__VA_ARGS__) + 1),
    APPLY_FOR_EVENTS(M)
#undef M
};
BucketLists global_bucket_lists(global_bucket_lists_array);

const LatencyEvent BucketLists::num_events = END;

void BucketLists::reset()
{
    if (bucket_lists)
    {
        for (LatencyEvent i = LatencyEvent(0); i < num_events; ++i)
            for (auto & bucket : bucket_lists[i])
                bucket.store(0, std::memory_order_relaxed);
    }
}

const char * getName(LatencyEvent event)
{
    static const char * strings[] =
    {
    #define M(NAME, ...) #NAME,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

const char * getDocumentation(LatencyEvent event)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION, ...) DOCUMENTATION "",
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

std::vector<Count> & getBucketBounds(LatencyEvent event)
{
    static std::vector<Count> all_buckets_bounds[] =
    {
    #define M(NAME, DOCUMENTATION, ...) {__VA_ARGS__, UINT_MAX},
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return all_buckets_bounds[event];
}

LatencyEvent end() { return END; }

void increment(LatencyEvent event, Count amount)
{
    global_bucket_lists.increment(event, amount);
}

void BucketLists::increment(LatencyEvent event, Count amount)
{
    auto & bucket_bounds = getBucketBounds(event);
    auto & bucket_list = this->bucket_lists[event];
    auto ind = std::lower_bound(bucket_bounds.begin(), bucket_bounds.end(), amount) - bucket_bounds.begin();
    bucket_list[ind].fetch_add(1, std::memory_order_relaxed);
}

}

#undef APPLY_FOR_EVENTS
