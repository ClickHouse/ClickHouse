#pragma once

#include <bit>

#include <base/types.h>

namespace DB
{

/// The staged records route by the two-level bucket of their key's hash, and their partitions nest in those
/// buckets, so the staging and the merge come in the same 256 buckets as the two-level hash tables.
inline constexpr size_t ADAPTIVE_AGGREGATION_NUM_BUCKETS = 256;

/// How the staged records of a session are partitioned, and the first chunk that the producers' staging buffers
/// start every partition with: both follow the same budget (see `forProducers`). A partition is a sub-range of a
/// two-level bucket: `partition = (bucket << sub_bits) | sub`, where `bucket` is the hash bits 24..31 the two-level
/// tables take it from (`TwoLevelHashTable::getBucketFromHash`), and `sub` the next `sub_bits` bits below them. The
/// bits are free: the 32-bit hashes of the two-level methods use 24..31 for the bucket and the low bits for the slot
/// inside a bucket's table, which keeps a partition's table well spread up to 2^20 cells. Since the partitions nest
/// in the buckets, every bucket-level contract of the merge holds unchanged.
class AdaptivePartitionLayout
{
public:
    /// The partitions of a bucket follow the number of producers, which is also the number of merge tasks. The merge
    /// tasks share the last-level cache, so the more of them there are, the more units a large bucket needs for each
    /// unit's table to stay in a task's share; every partition, though, costs the producers' appends and the merge's
    /// walk a fixed overhead per chunk, so a small bucket wants few. About the square root of the producers, half the
    /// bits of their count rounded up, balanced the two over the high-cardinality aggregations of ClickBench at 16 and
    /// 64 threads, with 4 and 8 partitions per bucket. An external-aggregation threshold reduces the subdivision so
    /// the first chunks of all streams fit within a quarter of it, and below one partition per bucket it reduces the
    /// first chunks, down to the smallest the staging buffer takes. The four free hash bits allow at most 16 partitions
    /// per bucket.
    static AdaptivePartitionLayout forProducers(size_t producers, size_t max_bytes_before_external_group_by);

    /// The first chunk of every partition in a producer's staging buffer (see `PartitionedRecordBuffer`).
    size_t firstChunkBytes() const { return first_chunk_bytes; }

    size_t partitionsPerBucket() const { return numPartitions() / ADAPTIVE_AGGREGATION_NUM_BUCKETS; }
    size_t numPartitions() const { return partition_mask + 1; }
    size_t partitionOf(UInt64 hash) const { return (hash >> partition_shift) & partition_mask; }

    /// The partitions of a bucket are consecutive: they start at `firstPartition(bucket)`, and `subOf` is a
    /// partition's place among them.
    size_t firstPartition(size_t bucket) const { return bucket * partitionsPerBucket(); }
    size_t subOf(size_t partition) const { return partition & (partitionsPerBucket() - 1); }
    size_t bucketOf(size_t partition) const { return partition / partitionsPerBucket(); }

private:
    /// The two-level tables take the bucket from the top `bucket_bits` of the low 32 bits of the hash. The kernels
    /// assert that their tables have `ADAPTIVE_AGGREGATION_NUM_BUCKETS` buckets, and the merge that every source
    /// cell of a bucket falls into that bucket's partitions.
    static constexpr size_t bucket_bits = std::countr_zero(ADAPTIVE_AGGREGATION_NUM_BUCKETS);

    AdaptivePartitionLayout(UInt8 sub_bits, size_t first_chunk_bytes_)
        : partition_mask(static_cast<UInt32>((ADAPTIVE_AGGREGATION_NUM_BUCKETS << sub_bits) - 1))
        , partition_shift(static_cast<UInt8>(32 - bucket_bits - sub_bits))
        , first_chunk_bytes(first_chunk_bytes_)
    {
    }

    /// Routing runs for every record and its prefetches, so the layout stores the ready shift and mask.
    UInt32 partition_mask;
    UInt8 partition_shift;

    size_t first_chunk_bytes;
};

}
