#include <Columns/ColumnString.h>
#include <Columns/ColumnUnique.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/scope_guard_safe.h>
#include <DataTypes/DataTypeString.h>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace DB::FailPoints
{
    extern const char reverse_index_build_index_throw[];
}

using namespace DB;

/// Mimics what happens during an asynchronous insert flush under memory pressure: the first insert into a
/// `LowCardinality` dictionary throws `MEMORY_LIMIT_EXCEEDED` while the reverse index is being built, the error
/// is handled by the caller, and the same dictionary is reused for the next entry. The index must either be
/// fully built or absent, never a non-null hash table paired with a null saved-hash array.
TEST(ReverseIndex, ExceptionWhileBuildingIndexLeavesNoHalfBuiltIndex)
{
    auto column = ColumnUnique<ColumnString>::create(DataTypeString());

    FailPointInjection::enableFailPoint(FailPoints::reverse_index_build_index_throw);
    try
    {
        column->uniqueInsertData("a", 1);
        FAIL() << "Expected an exception from the failpoint";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }
    FailPointInjection::disableFailPoint(FailPoints::reverse_index_build_index_throw);

    /// Index 0 is the default value (empty string).
    EXPECT_EQ(column->size(), 1u);
    EXPECT_EQ(column->uniqueInsertData("a", 1), 1u);
    EXPECT_EQ(column->uniqueInsertData("b", 1), 2u);
    EXPECT_EQ(column->uniqueInsertData("a", 1), 1u);
    EXPECT_EQ(column->uniqueInsertData("", 0), 0u);
    EXPECT_EQ(column->size(), 3u);
    EXPECT_EQ(column->getDataAt(1), "a");
    EXPECT_EQ(column->getDataAt(2), "b");
}

/// The same reuse after a failure inside `insert` itself: the key is already in the hash table and the string bytes are
/// already appended to `chars` when growing `offsets` fails. The dictionary must come back to the state before the
/// insert, so that the next inserted value does not get the failed one as a prefix and lookups stay correct.
TEST(ReverseIndex, ExceptionWhileInsertingDataKeepsDictionaryConsistent)
{
    MainThreadStatus::getInstance();
    auto & thread_tracker = CurrentThread::get().memory_tracker;

    auto column = ColumnUnique<ColumnString>::create(DataTypeString());
    ASSERT_EQ(column->uniqueInsertData("a", 1), 1u);

    /// Leave `offsets` without spare capacity and `chars` with plenty, so that growing `offsets` is the only
    /// allocation of the next insert.
    auto & nested = assert_cast<ColumnString &>(column->getNestedColumn()->assumeMutableRef());
    nested.getOffsets().shrink_to_fit();
    nested.getChars().reserve(nested.getChars().size() + 1024);

    const Int64 saved_untracked_limit = CurrentThread::get().untracked_memory_limit;
    const Int64 saved_thread_hard_limit = thread_tracker.getHardLimit();
    const Int64 saved_total_hard_limit = total_memory_tracker.getHardLimit();
    const UInt64 saved_min_alloc_to_throw = CurrentMemoryTracker::getMinAllocationSizeBytesToThrow();

    auto lift_limits = [&]
    {
        total_memory_tracker.setHardLimit(saved_total_hard_limit);
        thread_tracker.setHardLimit(saved_thread_hard_limit);
        CurrentThread::get().untracked_memory_limit = saved_untracked_limit;
        CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_alloc_to_throw);
    };
    SCOPE_EXIT_SAFE(lift_limits());

    /// Make even a tiny allocation reach the trackers and fail: no per-thread untracked buffer, limits of one byte.
    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1);
    CurrentThread::get().untracked_memory_limit = 0;
    CurrentThread::flushUntrackedMemory();
    total_memory_tracker.resetCounters();
    thread_tracker.resetCounters();
    total_memory_tracker.setHardLimit(1);
    thread_tracker.setHardLimit(1);

    bool thrown = false;
    int code = 0;
    try
    {
        column->uniqueInsertData("bb", 2);
    }
    catch (const Exception & e)
    {
        thrown = true;
        code = e.code();
    }

    /// Lift the limits before anything else, the assertions allocate.
    lift_limits();

    ASSERT_TRUE(thrown) << "the insert was expected to fail under the memory limit";
    EXPECT_EQ(code, ErrorCodes::MEMORY_LIMIT_EXCEEDED);

    EXPECT_EQ(column->size(), 2u);
    EXPECT_EQ(nested.getChars().size(), nested.getOffsets().back());

    EXPECT_EQ(column->uniqueInsertData("c", 1), 2u);
    EXPECT_EQ(column->getDataAt(2), "c");
    EXPECT_EQ(column->uniqueInsertData("bb", 2), 3u);
    EXPECT_EQ(column->getDataAt(3), "bb");
    EXPECT_EQ(column->uniqueInsertData("a", 1), 1u);
    EXPECT_EQ(column->uniqueInsertData("c", 1), 2u);
    EXPECT_EQ(column->size(), 4u);
}
