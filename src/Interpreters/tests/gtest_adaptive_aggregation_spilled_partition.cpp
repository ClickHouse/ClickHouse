#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/PartitionedRecordBuffer.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/tests/gtest_disk.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>
#include <Interpreters/TemporaryDataOnDisk.h>

#include <exception>
#include <random>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int TOO_MANY_ROWS_OR_BYTES;
}

/// The limit on temporary data breaks the stream in the middle of the first append. The next append to the same
/// partition must rethrow that exception instead of writing to the canceled stream.
TEST(AdaptiveAggregationSpilledPartition, AppendAfterBrokenWriteRethrowsItsError)
{
    auto disk = createDisk("adaptive_spilled_partition");
    TemporaryDataOnDiskSettings settings;
    settings.max_size_on_disk = 4096;
    settings.buffer_size = 1024;
    auto scope = std::make_shared<TemporaryDataOnDiskScope>(settings, std::make_shared<SingleDiskVolume>("temporary", disk));

    /// Random records do not compress, so one append writes more than the limit.
    PartitionedRecordBuffer records(/*num_partitions_=*/1, /*partitions_per_group_=*/1);
    std::mt19937_64 rng(42);
    for (size_t i = 0; i < 16; ++i)
    {
        char * record = records.append(0, 1000);
        for (size_t j = 0; j < 1000; ++j)
            record[j] = static_cast<char>(rng());
    }
    records.finishAppending();

    {
        AdaptiveAggregationSession::SpilledPartition spilled;
        auto append = [&]() -> std::exception_ptr
        {
            TemporaryAppendSpaceCheck space_check;
            try
            {
                spilled.append(scope, records, 0, 0, space_check);
            }
            catch (...)
            {
                return std::current_exception();
            }
            return nullptr;
        };

        auto first = append();
        ASSERT_TRUE(first);
        EXPECT_EQ(getExceptionErrorCode(first), ErrorCodes::TOO_MANY_ROWS_OR_BYTES) << getExceptionMessage(first, false);

        auto second = append();
        ASSERT_TRUE(second);
        EXPECT_EQ(getExceptionErrorCode(second), ErrorCodes::TOO_MANY_ROWS_OR_BYTES) << getExceptionMessage(second, false);
        EXPECT_EQ(second, first);
    }

    scope.reset();
    destroyDisk(disk);
}
