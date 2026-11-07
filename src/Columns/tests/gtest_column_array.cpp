#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
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

#include <algorithm>
#include <array>
#include <limits>
#include <numeric>
#include <random>
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

/// One array of two rows of a constant: the offsets match the nested column, so only the constant is wrong.
ColumnArray::MutablePtr createArrayOverConst()
{
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->getData().push_back(2);
    return ColumnArray::create(ColumnConst::create(ColumnUInt64::create(1, 42), 2), std::move(offsets));
}

ColumnArray::MutablePtr createDynamicArrayWithEmptyFirstRow()
{
    auto data = ColumnDynamic::create();
    data->insert(Field(UInt64(1) << 40));

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insertValue(0);
    offsets->insertValue(1);

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

TEST(ColumnArray, InsertManyFromRepeatedSmallArrays)
{
    auto source = createArray({42, 10, 20}, {0, 1, 3});
    auto destination = createArray({7}, {1});

    /// Repeating an empty array only appends the current offset.
    destination->insertManyFrom(*source, 0, 3);
    ASSERT_EQ(destination->size(), 4);
    EXPECT_EQ(destination->getData().size(), 1);
    EXPECT_EQ(destination->getSize(1), 0);
    EXPECT_EQ(destination->getSize(2), 0);
    EXPECT_EQ(destination->getSize(3), 0);

    /// A single-element array uses the nested column's bulk insertion path.
    destination->insertManyFrom(*source, 1, 3);
    ASSERT_EQ(destination->size(), 7);
    ASSERT_EQ(destination->getData().size(), 4);
    EXPECT_EQ(destination->getSize(4), 1);
    EXPECT_EQ(destination->getSize(5), 1);
    EXPECT_EQ(destination->getSize(6), 1);
    EXPECT_EQ(destination->getData().getUInt(1), 42);
    EXPECT_EQ(destination->getData().getUInt(2), 42);
    EXPECT_EQ(destination->getData().getUInt(3), 42);

    /// Arrays with multiple elements keep nested values interleaved.
    destination->insertManyFrom(*source, 2, 2);
    ASSERT_EQ(destination->size(), 9);
    ASSERT_EQ(destination->getData().size(), 8);
    EXPECT_EQ(destination->getSize(7), 2);
    EXPECT_EQ(destination->getSize(8), 2);
    EXPECT_EQ(destination->getData().getUInt(4), 10);
    EXPECT_EQ(destination->getData().getUInt(5), 20);
    EXPECT_EQ(destination->getData().getUInt(6), 10);
    EXPECT_EQ(destination->getData().getUInt(7), 20);

    /// Zero length remains a no-op even when the source position is invalid.
    destination->insertManyFrom(*source, 100, 0);
    EXPECT_EQ(destination->size(), 9);
    EXPECT_EQ(destination->getData().size(), 8);
}

TEST(ColumnArray, InsertManyFromMatchesScalarForSmallMultiElementArrays)
{
    auto source = createArray({10, 20, 30}, {3});

    for (size_t length : {size_t{2}, size_t{4}, size_t{16}})
    {
        auto bulk = createArray({7}, {1});
        auto scalar = createArray({7}, {1});

        bulk->insertManyFrom(*source, 0, length);
        for (size_t i = 0; i < length; ++i)
            scalar->insertFrom(*source, 0);

        ASSERT_EQ(bulk->size(), scalar->size()) << "length = " << length;
        ASSERT_EQ(bulk->getData().size(), scalar->getData().size()) << "length = " << length;
        ASSERT_EQ(bulk->getOffsets().size(), scalar->getOffsets().size()) << "length = " << length;

        for (size_t i = 0; i < bulk->size(); ++i)
        {
            EXPECT_EQ(bulk->getOffsets()[i], scalar->getOffsets()[i]) << "row = " << i << ", length = " << length;
            EXPECT_EQ((*bulk)[i], (*scalar)[i]) << "row = " << i << ", length = " << length;
        }
    }
}

TEST(ColumnArray, InsertManyFromMatchesScalarForDynamicNestedColumn)
{
    auto source = createDynamicArrayWithEmptyFirstRow();

    auto repeated = ColumnArray::create(ColumnDynamic::create());
    repeated->insertFrom(*source, 0);
    repeated->insertFrom(*source, 0);

    auto bulk = ColumnArray::create(ColumnDynamic::create());
    bulk->insertManyFrom(*source, 0, 2);

    ASSERT_EQ(bulk->size(), repeated->size());
    ASSERT_EQ(bulk->getData().size(), repeated->getData().size());
    ASSERT_EQ(bulk->getOffsets().size(), repeated->getOffsets().size());
    ASSERT_TRUE(bulk->getData().hasDynamicStructure());
    EXPECT_TRUE(bulk->dynamicStructureEquals(*repeated));

    for (size_t i = 0; i < bulk->size(); ++i)
    {
        EXPECT_EQ(bulk->getOffsets()[i], repeated->getOffsets()[i]);
        EXPECT_EQ((*bulk)[i], (*repeated)[i]);
    }
}

TEST(ColumnArray, InsertManyFromRejectsRowCountOverflow)
{
    auto source = createArray({}, {0});
    auto destination = createArray({}, {0});

    EXPECT_THROW(destination->insertManyFrom(*source, 0, std::numeric_limits<size_t>::max()), Exception);
    EXPECT_EQ(destination->size(), 1);
    EXPECT_EQ(destination->getData().size(), 0);
}

TEST(ColumnArray, InsertManyFromRejectsRowCountOverflowForSelfAlias)
{
    auto column = createArray({42}, {1});

    EXPECT_THROW(column->insertManyFrom(*column, 0, std::numeric_limits<size_t>::max()), Exception);
    EXPECT_EQ(column->size(), 1);
    EXPECT_EQ(column->getData().size(), 1);
    EXPECT_EQ(column->getOffsets().back(), 1);
    EXPECT_EQ(column->getData().getUInt(0), 42);
}

TEST(ColumnArray, InsertManyFromSelfString)
{
    auto data = ColumnString::create();
    const String first_value(1 << 20, 'x');
    const String second_value("y");
    data->insert(first_value);
    data->insert(second_value);

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->getData().push_back(data->size());
    auto column = ColumnArray::create(std::move(data), std::move(offsets));

    column->insertManyFrom(*column, 0, 2);

    ASSERT_EQ(column->size(), 3);
    ASSERT_EQ(column->getData().size(), 6);
    for (size_t i = 0; i < column->size(); ++i)
    {
        EXPECT_EQ(column->getData().getDataAt(2 * i), std::string_view(first_value));
        EXPECT_EQ(column->getData().getDataAt(2 * i + 1), std::string_view(second_value));
    }
}

TEST(ColumnArray, InsertManyFromPartiallyAliasedNullableString)
{
    auto shared_string = ColumnString::create();
    const String value(1 << 20, 'x');
    shared_string->insert(value);

    MutableColumnPtr source_nested = shared_string->getPtr();
    MutableColumnPtr destination_nested = shared_string->getPtr();

    auto source_null_map = ColumnUInt8::create();
    source_null_map->insertValue(0);
    auto destination_null_map = ColumnUInt8::create();
    destination_null_map->insertValue(0);

    auto source_data = ColumnNullable::create(std::move(source_nested), std::move(source_null_map));
    auto destination_data = ColumnNullable::create(std::move(destination_nested), std::move(destination_null_map));

    auto source_offsets = ColumnArray::ColumnOffsets::create();
    source_offsets->insertValue(1);
    auto source = ColumnArray::create(std::move(source_data), std::move(source_offsets));

    auto destination_offsets = ColumnArray::ColumnOffsets::create();
    destination_offsets->insertValue(1);
    auto destination = ColumnArray::create(std::move(destination_data), std::move(destination_offsets));

    ASSERT_NE(destination->getDataPtr().get(), source->getDataPtr().get());

    destination->insertManyFrom(*source, 0, 2);

    ASSERT_EQ(destination->size(), 3);
    const auto & destination_nullable = assert_cast<const ColumnNullable &>(destination->getData());
    const auto & destination_string = assert_cast<const ColumnString &>(destination_nullable.getNestedColumn());
    ASSERT_EQ(destination_string.size(), 3);
    ASSERT_EQ(destination_nullable.getNullMapData().size(), 3);
    for (size_t i = 0; i < destination->size(); ++i)
    {
        EXPECT_EQ(destination_string.getDataAt(i), std::string_view(value));
        EXPECT_EQ(destination_nullable.getNullMapData()[i], 0);
    }
}

TEST(ColumnArray, InsertManyFromNonAliasedString)
{
    auto source_data = ColumnString::create();
    source_data->insert(Field(String("prefix")));
    source_data->insert(Field(String("x\0y", 3)));
    source_data->insert(Field(String{}));

    auto source_offsets = ColumnArray::ColumnOffsets::create();
    source_offsets->insertValue(1);
    source_offsets->insertValue(2);
    source_offsets->insertValue(3);
    auto source = ColumnArray::create(std::move(source_data), std::move(source_offsets));

    auto createDestination = []
    {
        auto data = ColumnString::create();
        data->insert(Field(String("sentinel")));

        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insertValue(1);
        return ColumnArray::create(std::move(data), std::move(offsets));
    };

    auto bulk = createDestination();
    auto scalar = createDestination();
    ASSERT_NE(bulk->getDataPtr().get(), source->getDataPtr().get());

    bulk->insertManyFrom(*source, 1, 3);
    bulk->insertManyFrom(*source, 2, 2);
    for (size_t i = 0; i < 3; ++i)
        scalar->insertFrom(*source, 1);
    for (size_t i = 0; i < 2; ++i)
        scalar->insertFrom(*source, 2);

    ASSERT_EQ(bulk->size(), 6);
    ASSERT_EQ(bulk->getData().size(), 6);
    ASSERT_EQ(bulk->getOffsets().back(), 6);
    const auto & bulk_data = assert_cast<const ColumnString &>(bulk->getData());
    const auto & scalar_data = assert_cast<const ColumnString &>(scalar->getData());
    ASSERT_EQ(bulk_data.getChars().size(), 17);

    for (size_t i = 0; i < bulk->size(); ++i)
    {
        EXPECT_EQ(bulk->getSize(i), 1);
        EXPECT_EQ(bulk_data.getDataAt(i), scalar_data.getDataAt(i));
    }
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

TEST(ColumnArray, ConstNestedColumnIsRejected)
{
    EXPECT_THROW(createArrayOverConst(), Exception);
    EXPECT_THROW(ColumnArray::create(ColumnConst::create(ColumnUInt64::create(1, 42), 0)), Exception);
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

TEST(ColumnArrayDeathTest, ConstNestedColumnIsRejected)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    EXPECT_DEATH(createArrayOverConst(), "ColumnArray cannot have ColumnConst as its nested column");
    EXPECT_DEATH(
        ColumnArray::create(ColumnConst::create(ColumnUInt64::create(1, 42), 0)),
        "ColumnArray cannot have ColumnConst as its nested column");
}

#endif

namespace
{

/// 100 full 64-row blocks and a 50-row tail. Every 100th row (row 0, and row 6400 in the tail) is short and is the
/// one filtered out, so the passing rows are longer than average.
constexpr size_t filter_rows = 6450;
constexpr size_t filter_dropped_rows = 65;

bool isShortRow(size_t i)
{
    return i % 100 == 0;
}

IColumn::Filter dropShortRows()
{
    IColumn::Filter filt(filter_rows);
    for (size_t i = 0; i < filter_rows; ++i)
        filt[i] = !isShortRow(i);
    return filt;
}

ssize_t passingRows(const IColumn::Filter & filt)
{
    return std::count_if(filt.begin(), filt.end(), [](UInt8 x) { return x != 0; });
}

/// Arrays of length 1 in the short rows and 10 in the others, values distinct per element.
std::pair<std::vector<UInt64>, std::vector<ColumnArray::Offset>> shortAndLongArrays()
{
    std::vector<UInt64> values;
    std::vector<ColumnArray::Offset> offsets;
    for (size_t i = 0; i < filter_rows; ++i)
    {
        for (size_t j = 0; j < (isShortRow(i) ? 1 : 10); ++j)
            values.push_back(i * 10 + j);
        offsets.push_back(values.size());
    }
    return {values, offsets};
}

}

TEST(ColumnString, FilterReservesExactlyWhenShortRowsAreDropped)
{
    /// Short rows of length 1, then empty ones (the `s != ''` filter).
    for (size_t short_length : {1, 0})
    {
        auto column = ColumnString::create();
        for (size_t i = 0; i < filter_rows; ++i)
        {
            const String s(isShortRow(i) ? short_length : 100, static_cast<char>('a' + i % 26));
            column->insertData(s.data(), s.size());
        }

        const auto filt = dropShortRows();
        const auto res = column->filter(filt, passingRows(filt));
        const auto & res_string = assert_cast<const ColumnString &>(*res);

        /// Control, asserted first: the filter itself is right.
        ASSERT_EQ(res_string.size(), filter_rows - filter_dropped_rows);
        size_t row = 0;
        size_t kept_bytes = 0;
        for (size_t i = 0; i < filter_rows; ++i)
        {
            if (!filt[i])
                continue;
            ASSERT_EQ(res_string.getDataAt(row), column->getDataAt(i)) << "row " << i;
            kept_bytes += column->getDataAt(i).size();
            ++row;
        }
        ASSERT_EQ(res_string.getChars().size(), kept_bytes);

        /// A reservation proportional to the passing row count is too small here, and the buffer grows to a power of two.
        EXPECT_EQ(res_string.getChars().capacity(), res_string.getChars().size()) << "short_length " << short_length;
    }
}

TEST(ColumnArray, FilterReservesExactlyWhenShortArraysAreDropped)
{
    auto [values, offsets] = shortAndLongArrays();
    const auto column = createArray(values, offsets);

    const auto filt = dropShortRows();
    const auto res = column->filter(filt, passingRows(filt));
    const auto & res_data = assert_cast<const ColumnUInt64 &>(assert_cast<const ColumnArray &>(*res).getData()).getData();

    ASSERT_EQ(res->size(), filter_rows - filter_dropped_rows);
    size_t row = 0;
    for (size_t i = 0; i < filter_rows; ++i)
    {
        if (!filt[i])
            continue;
        ASSERT_EQ((*res)[row], (*column)[i]) << "row " << i;
        ++row;
    }
    ASSERT_EQ(res_data.size(), (filter_rows - filter_dropped_rows) * 10);

    EXPECT_EQ(res_data.capacity(), res_data.size());
}

TEST(ColumnArray, FilterNullableReservesExactlyWhenShortArraysAreDropped)
{
    auto [values, offsets] = shortAndLongArrays();
    auto nested = ColumnUInt64::create();
    auto null_map = ColumnUInt8::create();
    for (UInt64 value : values)
    {
        nested->getData().push_back(value);
        null_map->getData().push_back(value % 3 == 0);
    }
    auto offsets_column = ColumnArray::ColumnOffsets::create();
    for (auto offset : offsets)
        offsets_column->getData().push_back(offset);
    const auto column = ColumnArray::create(ColumnNullable::create(std::move(nested), std::move(null_map)), std::move(offsets_column));

    const auto filt = dropShortRows();
    const auto res = column->filter(filt, passingRows(filt));
    const auto & res_nullable = assert_cast<const ColumnNullable &>(assert_cast<const ColumnArray &>(*res).getData());
    const auto & res_data = assert_cast<const ColumnUInt64 &>(res_nullable.getNestedColumn()).getData();
    const auto & res_null_map = res_nullable.getNullMapData();

    ASSERT_EQ(res->size(), filter_rows - filter_dropped_rows);
    size_t row = 0;
    for (size_t i = 0; i < filter_rows; ++i)
    {
        if (!filt[i])
            continue;
        ASSERT_EQ((*res)[row], (*column)[i]) << "row " << i;
        ++row;
    }
    ASSERT_EQ(res_data.size(), (filter_rows - filter_dropped_rows) * 10);
    ASSERT_EQ(res_null_map.size(), res_data.size());

    EXPECT_EQ(res_data.capacity(), res_data.size());
}

TEST(ColumnArray, FilterNestedArraysWithApproximateHint)
{
    /// The outer row 0 holds 6000 of the 6064 inner arrays and is filtered out. The inner column gets the hint
    /// 64 * 6064 / 65 = 5970 while 6000 of its rows are filtered out.
    constexpr size_t inner_rows = 6064;
    std::vector<UInt64> values(inner_rows);
    std::vector<ColumnArray::Offset> inner_offsets(inner_rows);
    for (size_t i = 0; i < inner_rows; ++i)
    {
        values[i] = i;
        inner_offsets[i] = i + 1;
    }
    auto outer_offsets = ColumnArray::ColumnOffsets::create();
    outer_offsets->getData().push_back(6000);
    for (size_t i = 1; i <= 64; ++i)
        outer_offsets->getData().push_back(6000 + i);
    const auto column = ColumnArray::create(createArray(values, inner_offsets), std::move(outer_offsets));

    IColumn::Filter filt(65, 1);
    filt[0] = 0;
    const auto res = column->filter(filt, 64);

    ASSERT_EQ(res->size(), 64u);
    for (size_t i = 0; i < 64; ++i)
        ASSERT_EQ((*res)[i], (*column)[i + 1]) << "row " << i;
}

namespace
{

/// 1024 full 64-row filter blocks and a 48-row tail. Divisible by 16, so 7/8 and 15/16 of the rows are whole numbers.
constexpr size_t dense_filter_rows = 65584;

/// From this share of passing rows, a copying filter allocates the data of the result once with its exact size.
constexpr size_t dense_filter_numerator = 7;
constexpr size_t dense_filter_denominator = 8;

struct FilterResultView
{
    const IColumn::Offsets * offsets;
    const UInt8 * data;
    size_t data_bytes;
    size_t size;
    size_t capacity;
};

/// `ColumnString` or `ColumnArray` of `UInt64`.
FilterResultView viewFilterResult(const IColumn & column)
{
    if (column.getDataType() == TypeIndex::String)
    {
        const auto & column_string = assert_cast<const ColumnString &>(column);
        const auto & chars = column_string.getChars();
        return {&column_string.getOffsets(), chars.data(), chars.size(), chars.size(), chars.capacity()};
    }
    const auto & column_array = assert_cast<const ColumnArray &>(column);
    const auto & data = assert_cast<const ColumnUInt64 &>(column_array.getData()).getData();
    return {&column_array.getOffsets(), reinterpret_cast<const UInt8 *>(data.data()), data.size() * sizeof(UInt64), data.size(), data.capacity()};
}

void expectSameFilterResult(const IColumn & expected, const IColumn & actual, const std::string & what)
{
    const auto e = viewFilterResult(expected);
    const auto a = viewFilterResult(actual);
    ASSERT_EQ(a.offsets->size(), e.offsets->size()) << what;
    const auto offsets_mismatch = std::mismatch(e.offsets->begin(), e.offsets->end(), a.offsets->begin());
    ASSERT_TRUE(offsets_mismatch.first == e.offsets->end()) << what << ": offset " << offsets_mismatch.first - e.offsets->begin();
    ASSERT_EQ(a.data_bytes, e.data_bytes) << what;
    const auto data_mismatch = std::mismatch(e.data, e.data + e.data_bytes, a.data);
    ASSERT_TRUE(data_mismatch.first == e.data + e.data_bytes) << what << ": data byte " << data_mismatch.first - e.data;
}

}

TEST(ColumnString, FilterKeepingMostRowsMatchesRowByRowCopy)
{
    std::mt19937_64 rng(42); /// NOLINT(cert-msc32-c,cert-msc51-cpp,bugprone-random-generator-seed) a reproducible sequence
    const auto uniform = [&](size_t min, size_t max) { return std::uniform_int_distribution<size_t>(min, max)(rng); };

    std::vector<std::pair<std::string, ColumnPtr>> columns;
    const auto add_strings = [&](const std::string & name, const auto & length_of_row)
    {
        auto column = ColumnString::create();
        for (size_t i = 0; i < dense_filter_rows; ++i)
        {
            String s(length_of_row(i), '\0');
            for (size_t j = 0; j < s.size(); ++j)
                s[j] = static_cast<char>('a' + (i + j) % 26);
            column->insertData(s.data(), s.size());
        }
        columns.emplace_back(name, std::move(column));
    };
    add_strings("String U[0,3]", [&](size_t) { return uniform(0, 3); });
    add_strings("String U[20,120]", [&](size_t) { return uniform(20, 120); });
    /// With these empty rows, `s != ''` keeps exactly 7/8 and 15/16 of the rows.
    add_strings("String, every 8th row empty", [&](size_t i) { return i % 8 == 3 ? 0 : uniform(1, 120); });
    add_strings("String, every 16th row empty", [&](size_t i) { return i % 16 == 5 ? 0 : uniform(1, 120); });
    {
        auto data = ColumnUInt64::create();
        auto offsets = ColumnArray::ColumnOffsets::create();
        for (size_t i = 0; i < dense_filter_rows; ++i)
        {
            const size_t length = uniform(0, 8);
            for (size_t j = 0; j < length; ++j)
                data->getData().push_back(i * 16 + j);
            offsets->getData().push_back(data->size());
        }
        columns.emplace_back("Array(UInt64) U[0,8]", ColumnArray::create(std::move(data), std::move(offsets)));
    }

    std::vector<size_t> positions(dense_filter_rows);
    for (const auto & [name, column] : columns)
    {
        std::vector<std::pair<std::string, IColumn::Filter>> filters;
        for (size_t passing : std::array<size_t, 4>{57386, 61485, 64928, 65518})
        {
            std::iota(positions.begin(), positions.end(), size_t(0));
            std::shuffle(positions.begin(), positions.end(), rng);
            IColumn::Filter filt(dense_filter_rows, 0);
            for (size_t k = 0; k < passing; ++k)
                filt[positions[k]] = 1;
            filters.emplace_back("random " + std::to_string(passing) + " rows", std::move(filt));
        }
        const auto & offsets = *viewFilterResult(*column).offsets;
        IColumn::Filter non_empty(dense_filter_rows);
        for (size_t i = 0; i < dense_filter_rows; ++i)
            non_empty[i] = offsets[i] != (i == 0 ? 0 : offsets[i - 1]);
        filters.emplace_back("non-empty rows", std::move(non_empty));

        for (const auto & [filter_name, filt] : filters)
        {
            const std::string what = name + ", " + filter_name;
            const size_t passing = std::count_if(filt.begin(), filt.end(), [](UInt8 x) { return x != 0; });

            const auto expected = column->filter(filt, -1);
            const auto with_passing_rows = column->filter(filt, passing);
            const auto with_all_rows = column->filter(filt, dense_filter_rows);
            expectSameFilterResult(*expected, *with_passing_rows, what + ", hint = passing rows");
            expectSameFilterResult(*expected, *with_all_rows, what + ", hint = all rows");

            if (passing * dense_filter_denominator >= dense_filter_rows * dense_filter_numerator)
            {
                const auto view = viewFilterResult(*with_passing_rows);
                EXPECT_EQ(view.capacity, view.size) << what;
            }
        }
    }
}
