#include <Interpreters/AdaptiveAggregation/AdaptivePartitionLayout.h>

#include <algorithm>
#include <bit>

#include <base/defines.h>
#include <Common/PartitionedRecordBuffer.h>

namespace DB
{

AdaptivePartitionLayout AdaptivePartitionLayout::forProducers(size_t producers, size_t max_bytes_before_external_group_by)
{
    constexpr size_t max_sub_bits = 4;
    /// The chunk cap is the largest power of two at which one unused chunk in every partition of every
    /// producer fits in `unused_chunks_budget_bytes`, clamped to the bounds (see the header).
    constexpr size_t min_chunk_cap_bytes = 4 << 10;
    constexpr size_t max_chunk_cap_bytes = 32 << 10;
    constexpr size_t unused_chunks_budget_bytes = 128 << 20;
    /// The staging buffer takes a cap no smaller than the first chunk.
    static_assert(min_chunk_cap_bytes >= PartitionedRecordBuffer::max_first_chunk_bytes);

    chassert(producers > 0);
    size_t sub_bits = std::min<size_t>((std::bit_width(producers - 1) + 1) / 2, max_sub_bits);
    size_t first_chunk_bytes = PartitionedRecordBuffer::max_first_chunk_bytes;
    if (max_bytes_before_external_group_by)
    {
        /// The quarter of the threshold for the first chunks, as bytes per bucket and producer.
        const size_t bucket_budget
            = max_bytes_before_external_group_by / 4 / producers / ADAPTIVE_AGGREGATION_NUM_BUCKETS;
        if (const size_t affordable_per_bucket = bucket_budget / first_chunk_bytes)
            sub_bits = std::min<size_t>(sub_bits, std::countr_zero(std::bit_floor(affordable_per_bucket)));
        else
        {
            sub_bits = 0;
            first_chunk_bytes = std::max(PartitionedRecordBuffer::min_first_chunk_bytes, std::bit_floor(bucket_budget));
        }
    }
    const size_t producer_partitions = producers * (ADAPTIVE_AGGREGATION_NUM_BUCKETS << sub_bits);
    const size_t max_chunk_bytes
        = std::clamp(std::bit_floor(unused_chunks_budget_bytes / producer_partitions), min_chunk_cap_bytes, max_chunk_cap_bytes);
    return AdaptivePartitionLayout(static_cast<UInt8>(sub_bits), first_chunk_bytes, max_chunk_bytes);
}

}
