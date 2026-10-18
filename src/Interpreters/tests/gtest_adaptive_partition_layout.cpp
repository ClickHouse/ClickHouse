#include <Common/PartitionedRecordBuffer.h>
#include <Interpreters/AdaptiveAggregation/AdaptivePartitionLayout.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <utility>

using namespace DB;

/// Without an external-aggregation threshold the partitions of a bucket follow the number of producers, half the bits
/// of their count rounded up, at most 16, and every partition starts with the largest first chunk.
TEST(AdaptivePartitionLayout, PartitionsFollowProducers)
{
    for (const auto & [producers, partitions_per_bucket] :
         {std::pair<size_t, size_t>{1, 1}, {4, 2}, {16, 4}, {64, 8}, {1024, 16}})
    {
        const auto layout = AdaptivePartitionLayout::forProducers(producers, 0);
        EXPECT_EQ(layout.partitionsPerBucket(), partitions_per_bucket) << producers << " producers";
        EXPECT_EQ(layout.firstChunkBytes(), PartitionedRecordBuffer::max_first_chunk_bytes) << producers << " producers";
    }
}

/// An external-aggregation threshold reduces the partitions and then the first chunks, so that the first chunks of all
/// producers fit within a quarter of it, unless even the smallest first chunk in one partition per bucket does not.
TEST(AdaptivePartitionLayout, FirstChunksFitQuarterOfThreshold)
{
    for (size_t producers : {size_t{1}, size_t{4}, size_t{16}, size_t{64}, size_t{1024}})
    {
        const size_t smallest_footprint
            = producers * ADAPTIVE_AGGREGATION_NUM_BUCKETS * PartitionedRecordBuffer::min_first_chunk_bytes;
        for (size_t threshold = 1; threshold < (size_t{1} << 40); threshold = threshold * 3 + 1)
        {
            const auto layout = AdaptivePartitionLayout::forProducers(producers, threshold);
            const size_t first_chunks = producers * layout.numPartitions() * layout.firstChunkBytes();
            EXPECT_LE(first_chunks, std::max(threshold / 4, smallest_footprint)) << producers << " producers, " << threshold;
            EXPECT_GE(layout.firstChunkBytes(), PartitionedRecordBuffer::min_first_chunk_bytes);
            EXPECT_LE(layout.partitionsPerBucket(), AdaptivePartitionLayout::forProducers(producers, 0).partitionsPerBucket());
            if (layout.firstChunkBytes() < PartitionedRecordBuffer::max_first_chunk_bytes)
                EXPECT_EQ(layout.partitionsPerBucket(), 1) << producers << " producers, " << threshold;
        }
    }
}

/// The boundaries for 64 producers: a quarter of the threshold that affords two first chunks of 2 KiB per bucket and
/// producer keeps two partitions per bucket, one chunk keeps one partition, and less halves the first chunks until the
/// smallest, which a threshold of one byte takes.
TEST(AdaptivePartitionLayout, ThresholdBoundaries)
{
    constexpr size_t producers = 64;
    constexpr size_t one_chunk_per_bucket
        = 4 * producers * ADAPTIVE_AGGREGATION_NUM_BUCKETS * PartitionedRecordBuffer::max_first_chunk_bytes;

    auto layout = AdaptivePartitionLayout::forProducers(producers, 2 * one_chunk_per_bucket);
    EXPECT_EQ(layout.partitionsPerBucket(), 2);
    EXPECT_EQ(layout.firstChunkBytes(), PartitionedRecordBuffer::max_first_chunk_bytes);

    layout = AdaptivePartitionLayout::forProducers(producers, one_chunk_per_bucket);
    EXPECT_EQ(layout.partitionsPerBucket(), 1);
    EXPECT_EQ(layout.firstChunkBytes(), PartitionedRecordBuffer::max_first_chunk_bytes);

    layout = AdaptivePartitionLayout::forProducers(producers, one_chunk_per_bucket - 1);
    EXPECT_EQ(layout.partitionsPerBucket(), 1);
    EXPECT_EQ(layout.firstChunkBytes(), PartitionedRecordBuffer::max_first_chunk_bytes / 2);

    layout = AdaptivePartitionLayout::forProducers(producers, 1);
    EXPECT_EQ(layout.partitionsPerBucket(), 1);
    EXPECT_EQ(layout.firstChunkBytes(), PartitionedRecordBuffer::min_first_chunk_bytes);
}
