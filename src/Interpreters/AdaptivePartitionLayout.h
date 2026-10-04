#pragma once

#include <base/types.h>

namespace DB
{

/// The staged records route by the two-level bucket of their key's hash, and their partitions nest in those
/// buckets, so the staging and the merge come in the same 256 buckets as the two-level hash tables.
inline constexpr size_t ADAPTIVE_AGGREGATION_NUM_BUCKETS = 256;

/// How the staged records of a session are partitioned. A partition is a sub-range of a two-level bucket:
/// `partition = (bucket << sub_bits) | sub`, where `bucket` is hash bits 24..31, as for the two-level tables, and
/// `sub` the next `sub_bits` bits below them. The bits are free: the 32-bit hashes of the two-level methods use
/// 24..31 for the bucket and the low bits for the slot inside a bucket's table, which keeps a partition's table
/// well spread up to 2^20 cells. Since the partitions nest in the buckets, every bucket-level contract of the
/// merge holds unchanged.
class AdaptivePartitionLayout
{
public:
    /// The partitions of a bucket follow the number of producers, which is also the number of merge tasks. The merge
    /// tasks share the last-level cache, so the more of them there are, the more units a large bucket needs for each
    /// unit's table to stay in a task's share; every partition, though, costs the producers' appends and the merge's
    /// walk a fixed overhead per chunk, so a small bucket wants few. About the square root of the producers, half the
    /// bits of their count rounded up, balanced the two over the high-cardinality aggregations of ClickBench at 16 and
    /// 64 threads, with 4 and 8 partitions per bucket. With an external-aggregation threshold the first chunks of all
    /// streams also stay within a quarter of it, so the staging floor cannot hold the query over the threshold by
    /// itself. At most 16 partitions per bucket, the four free hash bits.
    static AdaptivePartitionLayout forProducers(size_t producers, size_t max_bytes_before_external_group_by);

    size_t partitionsPerBucket() const { return numPartitions() / ADAPTIVE_AGGREGATION_NUM_BUCKETS; }
    size_t numPartitions() const { return partition_mask + 1; }
    size_t partitionOf(UInt64 hash) const { return (hash >> partition_shift) & partition_mask; }

private:
    explicit AdaptivePartitionLayout(UInt8 sub_bits)
        : partition_mask(static_cast<UInt32>((ADAPTIVE_AGGREGATION_NUM_BUCKETS << sub_bits) - 1))
        , partition_shift(24 - sub_bits)
    {
    }

    /// Routing runs for every record and its prefetches, so the layout stores the ready shift and mask.
    UInt32 partition_mask;
    UInt8 partition_shift;
};

}
