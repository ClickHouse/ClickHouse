#include <Common/PartitionedRecordBuffer.h>

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <string>
#include <thread>

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

}

TEST(PartitionedRecordBuffer, ChunkGrowthAndLargeRecords)
{
    /// The last allocation group contains one partition. Mixed record sizes exercise chunk alignment,
    /// block rollover, and records allocated separately because they exceed the shared chunk size.
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

TEST(PartitionedRecordBuffer, ReleaseAndResumeAppending)
{
    PartitionedRecordBuffer buffer(2, 2);
    appendRecord(buffer, 0, "released");
    appendRecord(buffer, 1, "retained");
    buffer.finishAppending();
    buffer.releasePartition(0);

    EXPECT_FALSE(buffer.hasRecords(0));
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
    EXPECT_FALSE(buffer.hasRecords(0));
    EXPECT_FALSE(buffer.hasRecords(1));

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
        EXPECT_FALSE(buffer.hasRecords(partition));
}
