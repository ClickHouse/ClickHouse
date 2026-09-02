#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>

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

/// The part of the column that appending empty values must leave alone.
size_t nestedSize(const ColumnArray & column) { return column.getData().size(); }
size_t nestedSize(const ColumnString & column) { return column.getChars().size(); }

/// What the peak arms assert on, read off the column while it is still alive.
struct Grown
{
    Int64 peak = 0;
    size_t size = 0;
    size_t nested_size = 0;
    UInt64 last_offset = 0;
};

/// Grows a column of `create()` by `length` defaults on a dedicated thread and returns only numbers.
/// The column must live and die inside that thread: a free on another thread is charged to that other
/// thread's tracker, and a `ThreadStatus` of its own keeps this out of whatever `current_thread` the
/// rest of `unit_tests_dbms` set up.
template <typename Create>
Grown grow(size_t length, Create && create)
{
    Grown grown;

    std::thread measured([&]
    {
        ThreadStatus thread_status;
        auto & thread_tracker = CurrentThread::get().memory_tracker;
        /// An own tracker between the thread and the total one, so only what the column allocates is measured.
        MemoryTracker scope_tracker(&total_memory_tracker, VariableContext::Process, /*log_peak_memory_usage_in_destructor=*/false);
        MemoryTracker * prev_parent = thread_tracker.getParent();
        Int64 prev_untracked_limit = CurrentThread::get().untracked_memory_limit;

        /// Whatever the `ThreadStatus` constructor itself deferred must not be charged to `scope_tracker`.
        CurrentThread::flushUntrackedMemory();
        SCOPE_EXIT_SAFE({
            CurrentThread::flushUntrackedMemory();
            CurrentThread::get().untracked_memory_limit = prev_untracked_limit;
            thread_tracker.setParent(prev_parent);
        });

        /// Without this the offsets' reallocations are batched below the 4 MiB default and never reach the tracker.
        /// The tracker's counters are deliberately not reset: `resetCounters` would also drop its limits, and the
        /// peak is read from `scope_tracker` anyway.
        CurrentThread::get().untracked_memory_limit = 1;
        thread_tracker.setParent(&scope_tracker);

        {
            auto column = create();
            column->insertManyDefaults(length);

            grown.peak = scope_tracker.getPeak();
            grown.size = column->size();
            grown.nested_size = nestedSize(*column);
            grown.last_offset = column->getOffsets().back();
        }

        CurrentThread::flushUntrackedMemory();
    });
    measured.join();

    return grown;
}

/// Large enough that the offsets array (8 bytes per element) dwarfs everything else the body allocates.
constexpr size_t elements = 4'000'000;
constexpr Int64 offsets_bytes = static_cast<Int64>(elements) * sizeof(IColumn::Offset);

/// The shape that motivated the override: one default appended per row, many times.
constexpr size_t rows_appended_one_by_one = 400'000;

/// An offsets array holds 496 elements before it has to grow, so appending this many reallocates it.
constexpr size_t appended_past_initial_capacity = 4096;

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

TEST(ColumnArray, CutPreservesSharedLowCardinalityDictionary)
{
    auto dictionary_keys = ColumnUInt64::create();
    for (UInt64 value : {0, 10, 20, 30})
        dictionary_keys->insertValue(value);

    ColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(DataTypeUInt64(), std::move(dictionary_keys));

    auto indexes = ColumnUInt8::create();
    for (UInt8 index : {UInt8{1}, UInt8{2}, UInt8{3}, UInt8{1}, UInt8{2}})
        indexes->insertValue(index);

    auto nested = ColumnLowCardinality::create(dictionary, std::move(indexes), /* is_shared = */ true);
    auto offsets = ColumnArray::ColumnOffsets::create();
    for (ColumnArray::Offset offset : {2, 2, 5})
        offsets->insertValue(offset);

    auto column = ColumnArray::create(std::move(nested), std::move(offsets));
    auto cut_column = column->cut(1, 2);
    const auto & cut_array = assert_cast<const ColumnArray &>(*cut_column);
    const auto & cut_nested = assert_cast<const ColumnLowCardinality &>(cut_array.getData());

    ASSERT_TRUE(cut_nested.isSharedDictionary());
    EXPECT_EQ(cut_nested.getDictionaryPtr().get(), dictionary.get());
    ASSERT_EQ(cut_array.size(), 2);
    EXPECT_EQ(cut_array.getSize(0), 0);
    EXPECT_EQ(cut_array.getSize(1), 3);
    ASSERT_EQ(cut_nested.size(), 3);
    EXPECT_EQ(cut_nested.getUInt(0), 30);
    EXPECT_EQ(cut_nested.getUInt(1), 10);
    EXPECT_EQ(cut_nested.getUInt(2), 20);
}

TEST(ColumnArray, InsertManyDefaultsPreSizesOffsets)
{
    auto grown = grow(elements, [] { return ColumnArray::create(ColumnUInt64::create()); });

    /// Control, asserted first: the offsets really were appended, and the nested column was left alone.
    ASSERT_EQ(grown.size, elements);
    ASSERT_EQ(grown.nested_size, 0u);
    ASSERT_EQ(grown.last_offset, 0u);

    /// Asserted before the upper bound: holding the offsets needs at least their size, so a tracker that
    /// recorded nothing leaves the peak at 0 and would satisfy any upper bound, old implementation included.
    ASSERT_GE(grown.peak, offsets_bytes) << "tracker recorded nothing";

    /// One reallocation peaks at the final size; the doubling chain peaks at ~1.5x it.
    EXPECT_LT(grown.peak, offsets_bytes * 5 / 4) << "peak " << grown.peak << " vs offsets " << offsets_bytes;
}

TEST(ColumnArray, InsertManyDefaultsKeepsOffsetsAfterNonEmptyArrays)
{
    auto nested = ColumnUInt64::create();
    nested->insert(Field(UInt64(1)));
    nested->insert(Field(UInt64(2)));
    auto offsets = ColumnUInt64::create();
    offsets->insert(Field(UInt64(2)));
    auto column = ColumnArray::create(std::move(nested), std::move(offsets));

    /// More than the initial capacity, so the offsets are reallocated while the appended value is read.
    /// Read by reference instead of by value, that value would sit in the freed buffer.
    column->insertManyDefaults(appended_past_initial_capacity);

    ASSERT_EQ(column->size(), appended_past_initial_capacity + 1);
    ASSERT_EQ(column->getData().size(), 2u);
    ASSERT_EQ((*column)[0], Field(Array{UInt64(1), UInt64(2)}));
    for (size_t i = 1; i < appended_past_initial_capacity + 1; ++i)
    {
        ASSERT_EQ(column->getOffsets()[i], 2u) << "offset " << i;
        ASSERT_EQ((*column)[i], Field(Array{})) << "row " << i;
    }
}

TEST(ColumnArray, InsertManyDefaultsGrowsOffsetsGeometrically)
{
    /// Pre-sizing must stay geometric: `reserve_exact(size() + length)` would leave capacity == size after
    /// every call, so appending one default per row would reallocate on each of them.
    auto column = ColumnArray::create(ColumnUInt64::create());
    for (size_t i = 0; i < rows_appended_one_by_one; ++i)
        column->insertManyDefaults(1);

    ASSERT_EQ(column->size(), rows_appended_one_by_one);
    EXPECT_GT(column->getOffsets().capacity(), rows_appended_one_by_one);
}

TEST(ColumnString, InsertManyDefaultsPreSizesOffsets)
{
    auto grown = grow(elements, [] { return ColumnString::create(); });

    /// Control, asserted first: the offsets really were appended, and no characters were written.
    ASSERT_EQ(grown.size, elements);
    ASSERT_EQ(grown.nested_size, 0u);
    ASSERT_EQ(grown.last_offset, 0u);

    ASSERT_GE(grown.peak, offsets_bytes) << "tracker recorded nothing";

    EXPECT_LT(grown.peak, offsets_bytes * 5 / 4) << "peak " << grown.peak << " vs offsets " << offsets_bytes;
}

TEST(ColumnString, InsertManyDefaultsKeepsOffsetsAfterNonEmptyStrings)
{
    auto column = ColumnString::create();
    column->insert(Field(String("abc")));

    column->insertManyDefaults(appended_past_initial_capacity);

    ASSERT_EQ(column->size(), appended_past_initial_capacity + 1);
    ASSERT_EQ(column->getChars().size(), 3u); /// Strings here are not zero-terminated.
    ASSERT_EQ(column->getDataAt(0), std::string_view("abc"));
    for (size_t i = 1; i < appended_past_initial_capacity + 1; ++i)
    {
        ASSERT_EQ(column->getDataAt(i).size(), 0u) << "row " << i;
        ASSERT_EQ(column->getOffsets()[i], 3u) << "offset " << i;
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

#endif

/// A decreasing offset makes `sizeAt` underflow to a huge value even when the last offset matches
/// the size of the nested column, so the offsets are checked for monotonicity as well. The check is
/// a linear scan - a heavy assertion, so it only runs in debug and sanitizer builds, where a
/// LOGICAL_ERROR aborts the process: hence a death test.
#ifdef DEBUG_OR_SANITIZER_BUILD

TEST(ColumnArrayDeathTest, NonMonotonicOffsetsAreRejected)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    /// The extra parentheses keep the preprocessor from splitting the braced lists into macro arguments.

    /// The nested column is empty and the last offset is 0, but the first row claims one element.
    EXPECT_DEATH((createArray({}, {1, 0})), "not monotonically increasing");

    /// The last offset matches the nested column, but the offsets dip in the middle.
    EXPECT_DEATH((createArray({10, 20, 30}, {2, 1, 3})), "not monotonically increasing");
    EXPECT_DEATH((createArray({10, 20, 30}, {3, 0, 3})), "not monotonically increasing");
}

#endif
