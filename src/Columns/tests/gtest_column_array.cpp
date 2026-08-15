#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

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
