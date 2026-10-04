#include <Interpreters/AdaptivePartitionLayout.h>

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
    if (max_bytes_before_external_group_by)
    {
        const size_t streams = max_bytes_before_external_group_by / 4 / PartitionedRecordBuffer::first_chunk_bytes;
        const size_t affordable_per_bucket = std::max<size_t>(streams / producers / ADAPTIVE_AGGREGATION_NUM_BUCKETS, 1);
        sub_bits = std::min<size_t>(sub_bits, std::countr_zero(std::bit_floor(affordable_per_bucket)));
    }
    return AdaptivePartitionLayout(static_cast<UInt8>(sub_bits));
}

}
