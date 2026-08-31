#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>

#include <gtest/gtest.h>
#include <Common/Exception.h>

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
