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
    return AdaptivePartitionLayout(static_cast<UInt8>(sub_bits), first_chunk_bytes);
}

}
