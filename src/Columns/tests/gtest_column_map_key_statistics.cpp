#include <Columns/ColumnMap.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

ColumnPtr makeMapColumn()
{
    auto type = DataTypeFactory::instance().get("Map(String, UInt64)");
    auto column = type->createColumn();
    column->insert(Map{Tuple{Field("b"), Field(UInt64(2))}, Tuple{Field("a"), Field(UInt64(1))}});
    column->insert(Map{Tuple{Field("c"), Field(UInt64(3))}});
    return std::move(column);
}

}

TEST(ColumnMapKeyStatistics, CollectKeysOnDemand)
{
    auto column = makeMapColumn();
    auto sample = column->cloneEmpty();
    auto & sample_map = assert_cast<ColumnMap &>(*sample);

    sample_map.takeOrCalculateStatisticsFrom({column});
    ASSERT_TRUE(sample_map.getStatistics());
    EXPECT_TRUE(sample_map.getStatistics()->keys.empty());
    EXPECT_FALSE(sample_map.getStatistics()->collect_keys);

    sample_map.enableKeyCollection();
    sample_map.takeOrCalculateStatisticsFrom({column});
    ASSERT_TRUE(sample_map.getStatistics());
    EXPECT_TRUE(sample_map.getStatistics()->collect_keys);
    ASSERT_EQ(sample_map.getStatistics()->keys.size(), 3u);
    EXPECT_EQ(sample_map.getStatistics()->keys[0], Field("a"));
    EXPECT_EQ(sample_map.getStatistics()->keys[1], Field("b"));
    EXPECT_EQ(sample_map.getStatistics()->keys[2], Field("c"));
}

TEST(ColumnMapKeyStatistics, FrozenKeysAreCopiedNotRescanned)
{
    auto first = makeMapColumn();
    auto sample = first->cloneEmpty();
    auto & sample_map = assert_cast<ColumnMap &>(*sample);
    sample_map.enableKeyCollection();
    sample_map.takeOrCalculateStatisticsFrom({first});

    ColumnPtr sample_ptr = std::move(sample);

    auto type = DataTypeFactory::instance().get("Map(String, UInt64)");
    auto late = type->createColumn();
    late->insert(Map{Tuple{Field("z"), Field(UInt64(9))}});
    late->takeOrCalculateStatisticsFrom({sample_ptr});
    const auto & late_stats = assert_cast<const ColumnMap &>(*late).getStatistics();
    ASSERT_TRUE(late_stats);
    ASSERT_EQ(late_stats->keys.size(), 3u);
    EXPECT_EQ(late_stats->keys[0], Field("a"));
    EXPECT_EQ(late_stats->keys[2], Field("c"));
}
