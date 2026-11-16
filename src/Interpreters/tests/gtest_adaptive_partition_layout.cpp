#include <Common/PartitionedRecordBuffer.h>
#include <Interpreters/AdaptiveAggregation/AdaptivePartitionLayout.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <tuple>

using namespace DB;

/// Without an external-aggregation threshold the partitions of a bucket follow the number of producers,
/// half the bits of their count rounded up, at most 16, and every partition starts with the largest first
/// chunk. The chunk cap is the largest power of two at which one unused chunk in every partition of every
/// producer fits in 128 MiB, clamped to 4-32 KiB. For example, 8 producers with 4 partitions per bucket
/// have 8 * 1024 partitions, which afford 16 KiB each; 9 producers afford 14.2 KiB each, so they get
/// 8 KiB. The cases sit on both sides of each band's edge.
TEST(AdaptivePartitionLayout, PartitionsFollowProducers)
{
    for (const auto & [producers, partitions_per_bucket, max_chunk_bytes] : {
             std::tuple<size_t, size_t, size_t>{1, 1, 32 << 10},
             {4, 2, 32 << 10},
             {5, 4, 16 << 10},
             {8, 4, 16 << 10},
             {9, 4, 8 << 10},
             {16, 4, 8 << 10},
             {17, 8, 4 << 10},
             {64, 8, 4 << 10},
             {1024, 16, 4 << 10}})
    {
        const auto layout = AdaptivePartitionLayout::forProducers(producers, 0);
        EXPECT_EQ(layout.partitionsPerBucket(), partitions_per_bucket) << producers << " producers";
        EXPECT_EQ(layout.firstChunkBytes(), PartitionedRecordBuffer::max_first_chunk_bytes) << producers << " producers";
        EXPECT_EQ(layout.maxChunkBytes(), max_chunk_bytes) << producers << " producers";
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
            EXPECT_GE(layout.maxChunkBytes(), layout.firstChunkBytes()) << producers << " producers, " << threshold;
            EXPECT_LE(layout.partitionsPerBucket(), AdaptivePartitionLayout::forProducers(producers, 0).partitionsPerBucket());
            if (layout.firstChunkBytes() < PartitionedRecordBuffer::max_first_chunk_bytes)
                EXPECT_EQ(layout.partitionsPerBucket(), 1) << producers << " producers, " << threshold;
        }
    }
}

/// The boundaries for 64 producers: a quarter of the threshold that affords two first chunks of 2 KiB per
/// bucket and producer keeps two partitions per bucket, one chunk keeps one partition, and less halves the
/// first chunks until the smallest, which a threshold of one byte takes. The chunk cap follows the
/// partitions that remain: 64 producers with 512 partitions afford 4 KiB each within 128 MiB, and with 256
/// partitions 8 KiB.
TEST(AdaptivePartitionLayout, ThresholdBoundaries)
{
    constexpr size_t producers = 64;
    constexpr size_t one_chunk_per_bucket
        = 4 * producers * ADAPTIVE_AGGREGATION_NUM_BUCKETS * PartitionedRecordBuffer::max_first_chunk_bytes;

    auto layout = AdaptivePartitionLayout::forProducers(producers, 2 * one_chunk_per_bucket);
    EXPECT_EQ(layout.partitionsPerBucket(), 2);
    EXPECT_EQ(layout.firstChunkBytes(), PartitionedRecordBuffer::max_first_chunk_bytes);
    EXPECT_EQ(layout.maxChunkBytes(), size_t{4} << 10);

    layout = AdaptivePartitionLayout::forProducers(producers, one_chunk_per_bucket);
    EXPECT_EQ(layout.partitionsPerBucket(), 1);
    EXPECT_EQ(layout.firstChunkBytes(), PartitionedRecordBuffer::max_first_chunk_bytes);
    EXPECT_EQ(layout.maxChunkBytes(), size_t{8} << 10);

    layout = AdaptivePartitionLayout::forProducers(producers, one_chunk_per_bucket - 1);
    EXPECT_EQ(layout.partitionsPerBucket(), 1);
    EXPECT_EQ(layout.firstChunkBytes(), PartitionedRecordBuffer::max_first_chunk_bytes / 2);

    layout = AdaptivePartitionLayout::forProducers(producers, 1);
    EXPECT_EQ(layout.partitionsPerBucket(), 1);
    EXPECT_EQ(layout.firstChunkBytes(), PartitionedRecordBuffer::min_first_chunk_bytes);
}
