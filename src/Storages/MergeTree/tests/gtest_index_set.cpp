#include <Storages/MergeTree/MergeTreeIndexSet.h>

#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>

#include <gtest/gtest.h>

#include <memory>
#include <utility>


using namespace DB;

TEST(MergeTreeIndexSet, StopsCollectingOverfullGranule)
{
    Block index_sample_block
    {
        ColumnWithTypeAndName{ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "value"},
    };

    MergeTreeIndexAggregatorSet aggregator("idx", index_sample_block, 3);

    auto column = ColumnUInt64::create();
    auto & data = column->getData();
    for (UInt64 value = 0; value < 1000; ++value)
        data.push_back(value);

    Block block
    {
        ColumnWithTypeAndName{std::move(column), std::make_shared<DataTypeUInt64>(), "value"},
    };

    size_t pos = 0;
    aggregator.update(block, &pos, block.rows());

    EXPECT_EQ(pos, block.rows());

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleSet>(aggregator.getGranuleAndReset());
    ASSERT_NE(granule, nullptr);
    EXPECT_EQ(granule->size(), 4);
}
