#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/tests/gtest_disk.h>
#include <Interpreters/Set.h>
#include <Interpreters/TemporaryDataOnDisk.h>

#include <array>
#include <memory>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

using namespace DB;

namespace
{

constexpr size_t rows_per_source = 1024;
constexpr std::array<UInt64, 5> probe_keys{
    0, rows_per_source - 1, rows_per_source, 2 * rows_per_source - 1, 2 * rows_per_source};

std::shared_ptr<Set> makeReadySet(const DataTypePtr & type, UInt64 first_key)
{
    auto set = std::make_shared<Set>(SizeLimits{}, 0, false);
    set->setHeader({ColumnWithTypeAndName(type->createColumn(), type, "x")});

    auto column = ColumnUInt64::create();
    for (size_t i = 0; i < rows_per_source; ++i)
        column->insertValue(first_key + i);
    if (!set->insertFromColumns({std::move(column)}))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to prepare the source set for the memory-limit test");
    set->finishInsert();
    return set;
}

void expectMembership(const Set & set, const DataTypePtr & type, const std::array<UInt8, 5> & expected)
{
    auto column = ColumnUInt64::create();
    for (const UInt64 key : probe_keys)
        column->insertValue(key);
    auto result = set.execute({ColumnWithTypeAndName(std::move(column), type, "x")}, false);
    ASSERT_EQ(result->size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i)
        EXPECT_EQ(result->getUInt(i), expected[i]) << "key " << probe_keys[i];
}

}

/// A union of ready `Set` instances can run out of memory while its sources are still live.
/// The failed copy must release its own allocations and leave the original membership unchanged.
TEST(SetMemoryLimit, ReadyUnionReleasesMemoryAfterLimitException)
{
    MainThreadStatus::getInstance();

    const auto type = std::make_shared<DataTypeUInt64>();
    const ColumnsWithTypeAndName header{ColumnWithTypeAndName(type->createColumn(), type, "x")};
    const std::array<std::shared_ptr<Set>, 2> sources{
        makeReadySet(type, 0), makeReadySet(type, rows_per_source)};
    ASSERT_EQ(sources[0]->getTotalRowCount(), rows_per_source);
    ASSERT_EQ(sources[1]->getTotalRowCount(), rows_per_source);

    auto & thread_status = CurrentThread::get();
    auto & tracker = thread_status.memory_tracker;
    const Int64 saved_untracked_limit = thread_status.untracked_memory_limit;
    const Int64 saved_hard_limit = tracker.getHardLimit();
    const UInt64 saved_min_alloc_to_throw = CurrentMemoryTracker::getMinAllocationSizeBytesToThrow();
    SCOPE_EXIT_SAFE({
        tracker.setHardLimit(saved_hard_limit);
        thread_status.untracked_memory_limit = saved_untracked_limit;
        CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_alloc_to_throw);
    });

    /// Disable deferred accounting, then measure the cost of creating an empty destination.
    /// Leave enough room for that setup and the small preflight allocations, but not for
    /// growing the destination hash table while copying keys.
    thread_status.untracked_memory_limit = 0;
    CurrentThread::flushUntrackedMemory();
    const Int64 memory_before_calibration = tracker.get();
    Int64 setup_bytes = 0;
    {
        auto empty_destination = std::make_shared<Set>(SizeLimits(2 * rows_per_source, 0, OverflowMode::THROW), 0, false);
        empty_destination->setHeader(header);
        CurrentThread::flushUntrackedMemory();
        setup_bytes = tracker.get() - memory_before_calibration;
    }
    CurrentThread::flushUntrackedMemory();
    const Int64 memory_before = tracker.get();
    ASSERT_GT(setup_bytes, 1024);

    /// Regular hash-table allocations are always checked. Suppress limit checks only for
    /// small generic allocations made before the copy, including the new `Set` object.
    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(8192);
    tracker.setHardLimit(memory_before + setup_bytes + 1024);

    int exception_code = 0;
    try
    {
        (void)Set::tryUniteReadyFixedWidthSets(sources, header, 2 * rows_per_source);
    }
    catch (const Exception & exception)
    {
        exception_code = exception.code();
    }

    tracker.setHardLimit(saved_hard_limit);
    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_alloc_to_throw);
    CurrentThread::flushUntrackedMemory();
    const Int64 memory_after = tracker.get();
    thread_status.untracked_memory_limit = saved_untracked_limit;

    ASSERT_EQ(exception_code, ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    /// Allow small one-time library allocations, but not a retained hash-table buffer.
    EXPECT_LE(memory_after, memory_before + 1024);
    EXPECT_EQ(sources[0]->getTotalRowCount(), rows_per_source);
    EXPECT_EQ(sources[1]->getTotalRowCount(), rows_per_source);
    expectMembership(*sources[0], type, {1, 1, 0, 0, 0});
    expectMembership(*sources[1], type, {0, 0, 1, 1, 0});

    auto united = Set::tryUniteReadyFixedWidthSets(sources, header, 2 * rows_per_source);
    ASSERT_NE(united, nullptr);
    EXPECT_EQ(united->getTotalRowCount(), 2 * rows_per_source);
    expectMembership(*united, type, {1, 1, 1, 1, 0});
}

TEST(SetMemoryLimit, ReadyUnionCopiesSpillCapableSetUntilItSpills)
{
    MainThreadStatus::getInstance();

    const auto type = std::make_shared<DataTypeUInt64>();
    const ColumnsWithTypeAndName header{ColumnWithTypeAndName(type->createColumn(), type, "x")};
    auto disk = createDisk("ready_union_spill");
    SCOPE_EXIT_SAFE({ destroyDisk(disk); });

    auto source = std::make_shared<Set>(SizeLimits{}, 0, false);
    source->setHeader(header);
    const size_t spill_threshold = source->getTotalByteCount() + 1;
    SetSpillSettings spill_settings;
    spill_settings.tmp_data = std::make_shared<TemporaryDataOnDiskScope>(
        TemporaryDataOnDiskSettings{}, std::make_shared<SingleDiskVolume>("temporary", disk));
    spill_settings.max_bytes_before_external_set = spill_threshold;
    spill_settings.max_block_size = 64;
    source->setSpillSettings(std::move(spill_settings));

    auto column = ColumnUInt64::create();
    for (size_t i = 0; i < rows_per_source; ++i)
        column->insertValue(i);
    ASSERT_TRUE(source->insertFromColumns({std::move(column)}));
    source->finishInsert();
    ASSERT_EQ(source->getTotalRowCount(), rows_per_source);
    ASSERT_GE(source->getTotalByteCount(), spill_threshold);

    const std::array sources{source};
    auto united = Set::tryUniteReadyFixedWidthSets(sources, header, rows_per_source);
    ASSERT_NE(united, nullptr);
    expectMembership(*united, type, {1, 1, 0, 0, 0});

    EXPECT_GT(source->spill(1), 0);
    EXPECT_EQ(Set::tryUniteReadyFixedWidthSets(sources, header, rows_per_source), nullptr);
}
