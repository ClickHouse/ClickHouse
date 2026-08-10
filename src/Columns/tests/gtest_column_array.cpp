#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>

#include <thread>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

ColumnArray::MutablePtr createArray(std::vector<UInt64> data_values, std::vector<ColumnArray::Offset> offset_values)
{
    auto data = ColumnUInt64::create();
    for (UInt64 value : data_values)
        data->getData().push_back(value);

    auto offsets = ColumnArray::ColumnOffsets::create();
    for (ColumnArray::Offset offset : offset_values)
        offsets->getData().push_back(offset);

    return ColumnArray::create(std::move(data), std::move(offsets));
}

/// Appending `length` empty values only grows the offsets, so pre-sizing them to the final size costs one
/// reallocation. A push_back loop instead walks the doubling chain, and Allocator::realloc charges the new
/// block before releasing the old one, so its last step holds both at once. Peak is what separates the two:
/// the final capacity is identical. Returns the peak the tracker observed while `body` ran.
///
/// `body` runs in a dedicated thread so current_thread starts as nullptr, independent of whatever
/// ThreadStatus other gtests in unit_tests_dbms left behind -- and so this file leaves none behind either.
template <typename F>
Int64 peakOf(F && body)
{
    Int64 peak = 0;

    std::thread measured([&]
    {
        ThreadStatus thread_status;
        auto & thread_tracker = CurrentThread::get().memory_tracker;
        /// An own tracker between the thread and the total one, so only what `body` allocates is measured.
        MemoryTracker scope_tracker(&total_memory_tracker, VariableContext::Process, /*log_peak_memory_usage_in_destructor=*/false);
        MemoryTracker * prev_parent = thread_tracker.getParent();
        Int64 prev_untracked_limit = CurrentThread::get().untracked_memory_limit;

        /// Whatever the ThreadStatus constructor itself deferred must not be charged to `scope_tracker`.
        CurrentThread::flushUntrackedMemory();
        SCOPE_EXIT_SAFE({
            CurrentThread::flushUntrackedMemory();
            CurrentThread::get().untracked_memory_limit = prev_untracked_limit;
            thread_tracker.setParent(prev_parent);
        });

        /// Without this the offsets' reallocations are batched below the 4 MiB default and never reach the tracker.
        /// The tracker's counters are deliberately not reset: resetCounters would also drop its limits, and the
        /// peak is read from `scope_tracker` anyway.
        CurrentThread::get().untracked_memory_limit = 1;
        thread_tracker.setParent(&scope_tracker);

        body();

        CurrentThread::flushUntrackedMemory();
        peak = scope_tracker.getPeak();
    });
    measured.join();

    return peak;
}

/// Large enough that the offsets array (8 bytes per element) dwarfs everything else the body allocates.
constexpr size_t elements = 4'000'000;
constexpr Int64 offsets_bytes = static_cast<Int64>(elements) * sizeof(IColumn::Offset);

/// The shape that motivated the override: one default appended per row, many times.
constexpr size_t rows_appended_one_by_one = 400'000;

}

/// A ColumnArray is created from an already populated nested column and offsets, so its offsets
/// are always validated against the nested column, even if the nested column is empty.
TEST(ColumnArray, OffsetsConsistentWithNestedColumn)
{
    /// Both are empty - an array column without rows.
    EXPECT_EQ(createArray({}, {})->size(), 0);

    /// Rows with empty arrays: no elements, but the offsets are there.
    EXPECT_EQ(createArray({}, {0, 0, 0})->size(), 3);

    auto column = createArray({10, 20, 30}, {2, 2, 3});
    EXPECT_EQ(column->size(), 3);
    EXPECT_EQ(column->getSize(0), 2);
    EXPECT_EQ(column->getSize(1), 0);
    EXPECT_EQ(column->getSize(2), 1);
}

TEST(ColumnArray, InsertManyDefaultsPreSizesOffsets)
{
    auto column = ColumnArray::create(ColumnUInt64::create());

    Int64 peak = peakOf([&] { column->insertManyDefaults(elements); });

    /// Control, asserted first: the offsets really were appended, and the nested column was left alone.
    ASSERT_EQ(column->size(), elements);
    ASSERT_EQ(column->getData().size(), 0u);
    ASSERT_EQ(column->getOffsets().back(), 0u);

    /// One reallocation peaks at the final size; the doubling chain peaks at ~1.5x it.
    EXPECT_LT(peak, offsets_bytes * 5 / 4) << "peak " << peak << " vs offsets " << offsets_bytes;
}

TEST(ColumnArray, InsertManyDefaultsKeepsOffsetsAfterNonEmptyArrays)
{
    auto nested = ColumnUInt64::create();
    nested->insert(Field(UInt64(1)));
    nested->insert(Field(UInt64(2)));
    auto offsets = ColumnUInt64::create();
    offsets->insert(Field(UInt64(2)));
    auto column = ColumnArray::create(std::move(nested), std::move(offsets));

    column->insertManyDefaults(3);

    ASSERT_EQ(column->size(), 4u);
    ASSERT_EQ(column->getData().size(), 2u);
    ASSERT_EQ((*column)[0], Field(Array{UInt64(1), UInt64(2)}));
    for (size_t i = 1; i < 4; ++i)
    {
        EXPECT_EQ(column->getOffsets()[i], 2u) << "offset " << i;
        EXPECT_EQ((*column)[i], Field(Array{})) << "row " << i;
    }
}

TEST(ColumnArray, InsertManyDefaultsGrowsOffsetsGeometrically)
{
    /// Pre-sizing must stay geometric: reserve_exact(size() + length) would leave capacity == size after
    /// every call, so appending one default per row would reallocate on each of them.
    auto column = ColumnArray::create(ColumnUInt64::create());
    for (size_t i = 0; i < rows_appended_one_by_one; ++i)
        column->insertManyDefaults(1);

    ASSERT_EQ(column->size(), rows_appended_one_by_one);
    EXPECT_GT(column->getOffsets().capacity(), rows_appended_one_by_one);
}

TEST(ColumnString, InsertManyDefaultsPreSizesOffsets)
{
    auto column = ColumnString::create();

    Int64 peak = peakOf([&] { column->insertManyDefaults(elements); });

    /// Control, asserted first: the offsets really were appended, and no characters were written.
    ASSERT_EQ(column->size(), elements);
    ASSERT_EQ(column->getChars().size(), 0u);
    ASSERT_EQ(column->getOffsets().back(), 0u);

    EXPECT_LT(peak, offsets_bytes * 5 / 4) << "peak " << peak << " vs offsets " << offsets_bytes;
}

TEST(ColumnString, InsertManyDefaultsKeepsOffsetsAfterNonEmptyStrings)
{
    auto column = ColumnString::create();
    column->insert(Field(String("abc")));

    column->insertManyDefaults(3);

    ASSERT_EQ(column->size(), 4u);
    ASSERT_EQ(column->getChars().size(), 3u); /// Strings here are not zero-terminated.
    ASSERT_EQ(column->getDataAt(0), std::string_view("abc"));
    for (size_t i = 1; i < 4; ++i)
    {
        EXPECT_EQ(column->getDataAt(i).size(), 0u) << "row " << i;
        EXPECT_EQ(column->getOffsets()[i], 3u) << "offset " << i;
    }
}

TEST(ColumnString, InsertManyDefaultsGrowsOffsetsGeometrically)
{
    auto column = ColumnString::create();
    for (size_t i = 0; i < rows_appended_one_by_one; ++i)
        column->insertManyDefaults(1);

    ASSERT_EQ(column->size(), rows_appended_one_by_one);
    EXPECT_GT(column->getOffsets().capacity(), rows_appended_one_by_one);
}

/// Skipped under debug/sanitizers: LOGICAL_ERROR aborts there, so EXPECT_THROW can't catch it.
#ifndef DEBUG_OR_SANITIZER_BUILD

TEST(ColumnArray, InconsistentOffsetsAreRejected)
{
    /// The last offset is greater than the number of elements in the nested column.
    EXPECT_THROW(createArray({10, 20}, {3}), Exception);

    /// The last offset is less than the number of elements in the nested column.
    EXPECT_THROW(createArray({10, 20}, {1}), Exception);

    /// The nested column is empty, but the offsets promise elements:
    /// sizeAt(0) would report a size that is not there, and the consumers of the column
    /// would read the nested column out of bounds.
    EXPECT_THROW(createArray({}, {1}), Exception);
    EXPECT_THROW(createArray({}, {0, 1}), Exception);

    /// The offsets are empty (an implicit last offset of 0), but the nested column is not:
    /// the column would report zero rows while carrying hidden elements.
    EXPECT_THROW(createArray({10}, {}), Exception);
}

/// A decreasing offset makes `sizeAt` underflow to a huge value even when the last offset
/// matches the size of the nested column, so the offsets are checked for monotonicity as well.
TEST(ColumnArray, NonMonotonicOffsetsAreRejected)
{
    /// The nested column is empty and the last offset is 0, but the first row claims one element.
    EXPECT_THROW(createArray({}, {1, 0}), Exception);

    /// The last offset matches the nested column, but the offsets dip in the middle.
    EXPECT_THROW(createArray({10, 20, 30}, {2, 1, 3}), Exception);
    EXPECT_THROW(createArray({10, 20, 30}, {3, 0, 3}), Exception);
}

#endif
