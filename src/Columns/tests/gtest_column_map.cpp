#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <gtest/gtest.h>


using namespace DB;

static MutableColumnPtr createEmptyMapColumn()
{
    auto key_col = ColumnString::create();
    auto val_col = ColumnFloat64::create();
    auto tuple_col = ColumnTuple::create(Columns{std::move(key_col), std::move(val_col)});
    auto array_col = ColumnArray::create(std::move(tuple_col));
    return ColumnMap::create(std::move(array_col));
}

static MutableColumnPtr createMapColumnWithRows(size_t num_rows, size_t entries_per_row)
{
    auto key_col = ColumnString::create();
    auto val_col = ColumnFloat64::create();
    auto offsets_col = ColumnArray::ColumnOffsets::create();
    auto & offsets_data = offsets_col->getData();

    size_t current_offset = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        for (size_t j = 0; j < entries_per_row; ++j)
        {
            key_col->insert("key_" + std::to_string(i) + "_" + std::to_string(j));
            val_col->insert(static_cast<Float64>(i * 100 + j));
        }
        current_offset += entries_per_row;
        offsets_data.push_back(current_offset);
    }

    auto tuple_col = ColumnTuple::create(Columns{std::move(key_col), std::move(val_col)});
    auto array_col = ColumnArray::create(std::move(tuple_col), std::move(offsets_col));
    return ColumnMap::create(std::move(array_col));
}

/// Regression test: `calculateStatisticsForRange(0, 0)` on an empty ColumnMap
/// used to access `offsets[-1]` (out-of-bounds) before checking `start == end`.
/// This caused a SIGSEGV when `MergedData::insertChunk` processed an empty
/// ColumnMap chunk during `MergingSortedAlgorithm` execution.
TEST(ColumnMap, CalculateStatisticsForEmptyRange)
{
    auto map_col = createEmptyMapColumn();
    ASSERT_EQ(map_col->size(), 0u);
    ASSERT_TRUE(map_col->hasStatistics());

    auto stats = assert_cast<const ColumnMap &>(*map_col).calculateStatisticsForRange(0, 0);
    ASSERT_NE(stats, nullptr);
    ASSERT_EQ(stats->avg, 0);
    ASSERT_EQ(stats->count, 0u);
}

/// Verify `getOrCalculateStatistics` does not crash on an empty column.
TEST(ColumnMap, GetOrCalculateStatisticsEmpty)
{
    auto map_col = createEmptyMapColumn();
    auto stats = assert_cast<const ColumnMap &>(*map_col).getOrCalculateStatistics();
    ASSERT_NE(stats, nullptr);
    ASSERT_EQ(stats->avg, 0);
    ASSERT_EQ(stats->count, 0u);
}

/// Verify `takeOrCalculateStatisticsFrom` handles a mix of empty and
/// non-empty source columns without crashing.
TEST(ColumnMap, TakeOrCalculateStatisticsFromMixedSources)
{
    auto empty_col = createEmptyMapColumn();
    auto populated_col = createMapColumnWithRows(10, 3);

    VectorWithMemoryTracking<ColumnPtr> sources;
    sources.push_back(empty_col->getPtr());
    sources.push_back(populated_col->getPtr());
    sources.push_back(empty_col->getPtr());

    auto target = createEmptyMapColumn();
    assert_cast<ColumnMap &>(*target).takeOrCalculateStatisticsFrom(sources);

    auto stats = assert_cast<const ColumnMap &>(*target).getStatistics();
    ASSERT_NE(stats, nullptr);
    /// Only the populated column contributes: 10 rows, 3 entries each -> avg = 3.0
    ASSERT_DOUBLE_EQ(stats->avg, 3.0);
    ASSERT_EQ(stats->count, 10u);
}

/// Verify statistics are correct for a non-trivial range.
TEST(ColumnMap, CalculateStatisticsForNonEmptyRange)
{
    auto map_col = createMapColumnWithRows(5, 4);
    auto stats = assert_cast<const ColumnMap &>(*map_col).calculateStatisticsForRange(0, 5);
    ASSERT_NE(stats, nullptr);
    ASSERT_DOUBLE_EQ(stats->avg, 4.0);
    ASSERT_EQ(stats->count, 5u);
}

/// Verify statistics for a sub-range starting from a non-zero offset.
TEST(ColumnMap, CalculateStatisticsForSubRange)
{
    auto map_col = createMapColumnWithRows(10, 2);
    auto stats = assert_cast<const ColumnMap &>(*map_col).calculateStatisticsForRange(3, 7);
    ASSERT_NE(stats, nullptr);
    ASSERT_DOUBLE_EQ(stats->avg, 2.0);
    ASSERT_EQ(stats->count, 4u);
}
