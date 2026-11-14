#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/PartitionedRecordBuffer.h>
#include <Common/ThreadStatus.h>
#include <base/unit.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <functional>
#include <limits>
#include <string>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

void appendRecord(PartitionedRecordBuffer & buffer, size_t partition, const std::string & record)
{
    char * destination = buffer.append(partition, record.size());
    std::memcpy(destination, record.data(), record.size());
    std::memset(destination + record.size(), 0, PartitionedRecordBuffer::tail_padding_bytes);
}

std::string readPartition(const PartitionedRecordBuffer & buffer, size_t partition)
{
    std::string result;
    buffer.forEachChunk(partition, [&](std::string_view chunk) { result.append(chunk); });
    return result;
}

/// Runs `body` in a thread with untracked-memory batching disabled, so its memory tracker sees each block
/// of a buffer as it is allocated.
void runTracked(const std::function<void(const MemoryTracker &)> & body)
{
    std::thread([&]
    {
        ThreadStatus thread_status;
        thread_status.untracked_memory_limit = 0;
        body(thread_status.memory_tracker);
    }).join();
}

/// The memory an append allocates: the block it starts, if any, since a record that fits a shared chunk
/// allocates nothing else.
size_t allocatedByAppend(
    const MemoryTracker & tracker, PartitionedRecordBuffer & buffer, size_t partition, const std::string & record)
{
    const Int64 before = tracker.get();
    appendRecord(buffer, partition, record);
    return static_cast<size_t>(tracker.get() - before);
}

}

TEST(PartitionedRecordBuffer, ChunkGrowthAndLargeRecords)
{
    /// The last allocation group contains one partition. Mixed record sizes exercise chunk alignment,
    /// block rollover, and records allocated separately because they take more than half of the chunk cap.
    PartitionedRecordBuffer buffer(3, 2);
    std::array<std::string, 3> expected;
    for (size_t row = 0; row < 300; ++row)
    {
        const size_t partition = row % expected.size();
        const std::string record(row % 7 == 0 ? 40001 : 2001 + row, static_cast<char>('a' + row % 26));
        appendRecord(buffer, partition, record);
        expected[partition] += record;
    }
    buffer.finishAppending();

    for (size_t partition = 0; partition < expected.size(); ++partition)
    {
        EXPECT_EQ(buffer.recordsOf(partition), 100);
        EXPECT_EQ(readPartition(buffer, partition), expected[partition]);
    }
}

TEST(PartitionedRecordBuffer, ChunksDoubleUpToTheCap)
{
    /// A partition's chunks double from the first chunk up to the cap the buffer is given, and no further: the largest
    /// chunk of a long partition holds more than half the cap and no more than the cap.
    for (const size_t cap : {PartitionedRecordBuffer::default_max_chunk_bytes, PartitionedRecordBuffer::default_max_chunk_bytes / 2})
    {
        PartitionedRecordBuffer buffer(1, 1, PartitionedRecordBuffer::max_first_chunk_bytes, cap);
        std::string expected;
        for (size_t row = 0; row < 1000; ++row)
        {
            const std::string record(100, static_cast<char>('a' + row % 26));
            appendRecord(buffer, 0, record);
            expected += record;
        }
        buffer.finishAppending();

        size_t largest_chunk = 0;
        buffer.forEachChunk(0, [&](std::string_view chunk) { largest_chunk = std::max(largest_chunk, chunk.size()); });
        EXPECT_GT(largest_chunk, cap / 2) << cap;
        EXPECT_LE(largest_chunk, cap) << cap;
        EXPECT_EQ(readPartition(buffer, 0), expected);
    }
}

TEST(PartitionedRecordBuffer, RecordsOverHalfTheCapTakeTheirOwnSize)
{
    /// A record of more than half the chunk cap gets a chunk of its own size, so a partition of such records takes
    /// about the records' bytes, where capped chunks holding one record each would take the whole cap per record.
    constexpr size_t record_size = PartitionedRecordBuffer::default_max_chunk_bytes / 2 + 1000;
    constexpr size_t records = 10;
    PartitionedRecordBuffer buffer(1, 1);
    std::string expected;
    for (size_t row = 0; row < records; ++row)
    {
        const std::string record(record_size, static_cast<char>('a' + row));
        appendRecord(buffer, 0, record);
        expected += record;
    }
    buffer.finishAppending();

    /// Each chunk adds its header and the tail padding to the record.
    EXPECT_LT(buffer.allocatedChunkBytes(), records * (record_size + 128));
    EXPECT_EQ(buffer.recordsOf(0), records);
    EXPECT_EQ(readPartition(buffer, 0), expected);
}

TEST(PartitionedRecordBuffer, RecordTooLargeForAChunk)
{
    /// A chunk keeps its size in 32 bits, so a record that cannot fit one is rejected before anything is allocated.
    /// The partition keeps its records and accepts the next one.
    PartitionedRecordBuffer buffer(1, 1);
    appendRecord(buffer, 0, "first");
    EXPECT_THROW(buffer.append(0, std::numeric_limits<UInt32>::max()), Exception);
    appendRecord(buffer, 0, "second");
    buffer.finishAppending();

    EXPECT_EQ(buffer.recordsOf(0), 2);
    EXPECT_EQ(readPartition(buffer, 0), "firstsecond");
}

TEST(PartitionedRecordBuffer, ReleaseAndResumeAppending)
{
    PartitionedRecordBuffer buffer(2, 2);
    appendRecord(buffer, 0, "released");
    appendRecord(buffer, 1, "retained");
    buffer.finishAppending();
    buffer.releasePartition(0);

    EXPECT_EQ(buffer.recordsOf(0), 0);
    EXPECT_EQ(readPartition(buffer, 1), "retained");

    appendRecord(buffer, 0, "new");
    appendRecord(buffer, 1, " record");
    buffer.finishAppending();
    EXPECT_EQ(readPartition(buffer, 0), "new");
    EXPECT_EQ(readPartition(buffer, 1), "retained record");
    EXPECT_EQ(buffer.recordsOf(0), 1);
    EXPECT_EQ(buffer.recordsOf(1), 2);

    /// Clearing an active producer also releases its claims on partially carved blocks.
    appendRecord(buffer, 0, std::string(10000, 'x'));
    buffer.clear();
    EXPECT_EQ(buffer.allocatedChunkBytes(), 0);
    EXPECT_EQ(buffer.recordsOf(0), 0);
    EXPECT_EQ(buffer.recordsOf(1), 0);

    appendRecord(buffer, 1, "reused");
    buffer.finishAppending();
    EXPECT_EQ(readPartition(buffer, 1), "reused");
    EXPECT_EQ(buffer.recordsOf(1), 1);
}

TEST(PartitionedRecordBuffer, ConcurrentPartitionRelease)
{
    constexpr size_t partitions = 33;
    constexpr size_t num_consumers = 4;
    PartitionedRecordBuffer buffer(partitions, 16);
    for (size_t partition = 0; partition < partitions; ++partition)
        appendRecord(buffer, partition, std::string(2001, static_cast<char>(partition)));
    buffer.finishAppending();

    {
        std::array<std::jthread, num_consumers> consumers;
        for (size_t consumer = 0; consumer < num_consumers; ++consumer)
        {
            consumers[consumer] = std::jthread([&buffer, consumer]
            {
                for (size_t partition = consumer; partition < partitions; partition += num_consumers)
                {
                    EXPECT_EQ(readPartition(buffer, partition), std::string(2001, static_cast<char>(partition)));
                    buffer.releasePartition(partition);
                }
            });
        }
    }

    for (size_t partition = 0; partition < partitions; ++partition)
        EXPECT_EQ(buffer.recordsOf(partition), 0);
}

TEST(PartitionedRecordBuffer, RecordCountsWhileAppending)
{
    PartitionedRecordBuffer buffer(1, 1);
    const std::string record(2001, 'x');
    for (size_t row = 0; row < 300; ++row)
    {
        appendRecord(buffer, 0, record);
        EXPECT_EQ(buffer.recordsOf(0), row + 1);
    }

    /// Finishing publishes the last chunk's bytes without changing the partition's record count.
    buffer.finishAppending();
    buffer.finishAppending();
    EXPECT_EQ(buffer.recordsOf(0), 300);

    appendRecord(buffer, 0, std::string(40001, 'y'));
    buffer.finishAppending();
    EXPECT_EQ(buffer.recordsOf(0), 301);
    buffer.releasePartition(0);
    EXPECT_EQ(buffer.recordsOf(0), 0);
}

TEST(PartitionedRecordBuffer, GroupBlocksGrowFromTheFirstChunks)
{
    runTracked([](const MemoryTracker & tracker)
    {
        /// Two groups of 16 partitions and a last group of 4. A group's first block takes the first
        /// chunks of its partitions.
        PartitionedRecordBuffer buffer(36, 16);
        const std::string record(1000, 'x');
        constexpr size_t later_blocks = 5;
        std::vector<size_t> blocks;
        blocks.reserve(later_blocks);
        const Int64 initial = tracker.get();
        EXPECT_EQ(allocatedByAppend(tracker, buffer, 0, record), 32_KiB);
        EXPECT_EQ(allocatedByAppend(tracker, buffer, 16, record), 32_KiB);
        EXPECT_EQ(allocatedByAppend(tracker, buffer, 32, record), 8_KiB);

        /// Each later block is as large as the group's blocks together, up to the cap of 256 KiB, so
        /// the group's total doubles up to the cap and then grows by it.
        while (blocks.size() < later_blocks)
            if (const size_t allocated = allocatedByAppend(tracker, buffer, 0, record))
                blocks.push_back(allocated);
        EXPECT_EQ(blocks, (std::vector<size_t>{32_KiB, 64_KiB, 128_KiB, 256_KiB, 256_KiB}));

        /// Appends resumed after `finishAppending` continue the growth: a new chunk of the group
        /// starts a block of the cap.
        buffer.finishAppending();
        EXPECT_EQ(allocatedByAppend(tracker, buffer, 1, record), 256_KiB);

        /// `clear` frees every block and starts every group over from its first block.
        buffer.clear();
        EXPECT_EQ(tracker.get(), initial);
        EXPECT_EQ(allocatedByAppend(tracker, buffer, 0, record), 32_KiB);
    });
}

TEST(PartitionedRecordBuffer, FirstBlockSizes)
{
    runTracked([](const MemoryTracker & tracker)
    {
        /// The first block rounds the first chunks of its group up to a power of two: those of 24
        /// partitions take 48 KiB. The cap of 256 KiB bounds the first chunks of 256 partitions, which
        /// take 512 KiB.
        const std::string record(1000, 'x');
        PartitionedRecordBuffer rounded(24, 24);
        EXPECT_EQ(allocatedByAppend(tracker, rounded, 0, record), 64_KiB);
        PartitionedRecordBuffer capped(256, 256);
        EXPECT_EQ(allocatedByAppend(tracker, capped, 0, record), 256_KiB);

        /// A chunk larger than the group's next block gets a block of its own size: the first block of
        /// a group of one partition is 2 KiB, below the chunk of a record of 3000 bytes.
        PartitionedRecordBuffer single(1, 1);
        const size_t allocated = allocatedByAppend(tracker, single, 0, std::string(3000, 'x'));
        EXPECT_GT(allocated, 3000 + PartitionedRecordBuffer::tail_padding_bytes);
        EXPECT_LT(allocated, 4_KiB);
    });
}

TEST(PartitionedRecordBuffer, SmallFirstChunks)
{
    runTracked([](const MemoryTracker & tracker)
    {
        /// A small record in each of 256 partitions, in groups of 16. With the largest first chunk, every group's
        /// first block takes the first chunks of its partitions, 32 KiB. With the smallest, a group's first block of
        /// 2 KiB holds 15 first chunks besides the block's header, and the 16th starts a second block of 2 KiB.
        const auto allocated_by_small_records = [&](size_t first_chunk_bytes)
        {
            PartitionedRecordBuffer buffer(256, 16, first_chunk_bytes);
            const Int64 initial = tracker.get();
            for (size_t partition = 0; partition < 256; ++partition)
                appendRecord(buffer, partition, "small");
            return static_cast<size_t>(tracker.get() - initial);
        };
        EXPECT_EQ(allocated_by_small_records(PartitionedRecordBuffer::max_first_chunk_bytes), 16 * 32_KiB);
        EXPECT_EQ(allocated_by_small_records(PartitionedRecordBuffer::min_first_chunk_bytes), 16 * 4_KiB);

        /// A record larger than the smallest first chunk takes a chunk of its own size, and the records read back in
        /// order.
        PartitionedRecordBuffer buffer(2, 2, PartitionedRecordBuffer::min_first_chunk_bytes);
        appendRecord(buffer, 0, "first");
        appendRecord(buffer, 0, std::string(1000, 'x'));
        appendRecord(buffer, 0, "last");
        buffer.finishAppending();
        EXPECT_EQ(readPartition(buffer, 0), "first" + std::string(1000, 'x') + "last");
    });
}
