#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>

#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// Appending `length` empty values only grows the offsets, so pre-sizing them to the final size costs one
/// reallocation. A push_back loop instead walks the doubling chain, and Allocator::realloc charges the new
/// block before releasing the old one, so its last step holds both at once. Peak is what separates the two:
/// the final capacity is identical. Returns the peak the tracker observed while `body` ran.
template <typename F>
Int64 peakOf(F && body)
{
    MainThreadStatus::getInstance();
    auto & thread_tracker = CurrentThread::get().memory_tracker;
    /// An own tracker between the thread and the total one, so only what `body` allocates is measured.
    MemoryTracker scope_tracker(&total_memory_tracker, VariableContext::Process, /*log_peak_memory_usage_in_destructor=*/false);
    MemoryTracker * prev_parent = thread_tracker.getParent();
    Int64 prev_untracked_limit = CurrentThread::get().untracked_memory_limit;

    CurrentThread::flushUntrackedMemory();
    SCOPE_EXIT_SAFE({
        CurrentThread::flushUntrackedMemory();
        CurrentThread::get().untracked_memory_limit = prev_untracked_limit;
        thread_tracker.setParent(prev_parent);
    });

    /// Without this the offsets' reallocations are batched below the 4 MiB default and never reach the tracker.
    /// The thread tracker's own counters are deliberately not reset: the peak is read from `scope_tracker`, and
    /// zeroing them here would leave the amount negative once the caller frees the column (Thread does not saturate).
    CurrentThread::get().untracked_memory_limit = 1;
    thread_tracker.setParent(&scope_tracker);

    body();

    CurrentThread::flushUntrackedMemory();
    return scope_tracker.getPeak();
}

/// Large enough that the offsets array (8 bytes per element) dwarfs everything else the body allocates.
constexpr size_t elements = 4'000'000;
constexpr Int64 offsets_bytes = static_cast<Int64>(elements) * sizeof(IColumn::Offset);

/// The shape that motivated the override: one default appended per row, many times.
constexpr size_t rows_appended_one_by_one = 400'000;

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
