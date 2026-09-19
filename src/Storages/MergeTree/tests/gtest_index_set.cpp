#include <Storages/MergeTree/MergeTreeIndexSet.h>

#include <Columns/ColumnVector.h>
#include <Core/Range.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

#include <memory>
#include <string_view>
#include <utility>


using namespace DB;

namespace
{

/// Once more than this many rows are collected the aggregator stops collecting, and the granule
/// bounds no longer follow from the inserted values.
constexpr size_t MAX_ROWS = 100;

ColumnWithTypeAndName uint64Column(const String & name, UInt64 begin, UInt64 end)
{
    auto column = ColumnUInt64::create();
    for (UInt64 value = begin; value < end; ++value)
        column->getData().push_back(value);
    return {std::move(column), std::make_shared<DataTypeUInt64>(), name};
}

ColumnWithTypeAndName dynamicColumn(const String & name, UInt64 begin, UInt64 end)
{
    auto type = DataTypeFactory::instance().get("Dynamic");
    auto column = type->createColumn();
    for (UInt64 value = begin; value < end; ++value)
        column->insert(Field(value));
    return {std::move(column), type, name};
}

ColumnWithTypeAndName tupleWithDynamicColumn(const String & name, UInt64 begin, UInt64 end)
{
    auto type = DataTypeFactory::instance().get("Tuple(UInt64, Dynamic)");
    auto column = type->createColumn();
    for (UInt64 value = begin; value < end; ++value)
        column->insert(Field(Tuple{Field(value), Field(value)}));
    return {std::move(column), type, name};
}

ColumnWithTypeAndName tupleWithVariantColumn(const String & name, UInt64 begin, UInt64 end)
{
    auto type = DataTypeFactory::instance().get("Tuple(UInt64, Variant(UInt64, String))");
    auto column = type->createColumn();
    for (UInt64 value = begin; value < end; ++value)
        column->insert(Field(Tuple{Field(value), Field(value)}));
    return {std::move(column), type, name};
}

void expectMeasuredRange(std::string_view what, const Range & range, UInt64 expected_left, UInt64 expected_right)
{
    SCOPED_TRACE(what);
    ASSERT_FALSE(range.left.isNegativeInfinity());
    ASSERT_FALSE(range.right.isPositiveInfinity());
    EXPECT_EQ(range.left.safeGet<UInt64>(), expected_left);
    EXPECT_EQ(range.right.safeGet<UInt64>(), expected_right);
    EXPECT_TRUE(range.left_included);
    EXPECT_TRUE(range.right_included);
}

void expectWholeUniverse(std::string_view what, const Range & range)
{
    SCOPED_TRACE(what);
    EXPECT_TRUE(range.left.isNegativeInfinity());
    EXPECT_TRUE(range.right.isPositiveInfinity());
    /// Both bounds must be inclusive: the exclusive universe is contained in the range an `IS NULL`
    /// atom carries, and KeyCondition inverts that mask, so the granule would always be dropped.
    EXPECT_TRUE(range.left_included);
    EXPECT_TRUE(range.right_included);
}

void updateAggregator(MergeTreeIndexAggregatorSet & aggregator, const Block & block)
{
    size_t pos = 0;
    aggregator.update(block, &pos, block.rows());
    ASSERT_EQ(pos, block.rows());
}

std::shared_ptr<MergeTreeIndexGranuleSet> takeGranule(MergeTreeIndexAggregatorSet & aggregator)
{
    return std::dynamic_pointer_cast<MergeTreeIndexGranuleSet>(aggregator.getGranuleAndReset());
}

/// The granule's own serialization, i.e. the path a query takes when it reads the index from a part.
/// Version 1 is the only version the granule accepts.
void reserializeGranule(const MergeTreeIndexGranuleSet & granule, MergeTreeIndexGranuleSet & restored)
{
    WriteBufferFromOwnString out;
    granule.serializeBinary(out);
    ReadBufferFromString in(out.str());
    restored.deserializeBinary(in, 1);
}

}

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

TEST(MergeTreeIndexSet, GranuleRangeForMeasurableColumn)
{
    const String index_name = "idx";
    const Block block{uint64Column("value", 0, 10)};
    const Block index_sample_block = block.cloneEmpty();

    MergeTreeIndexAggregatorSet aggregator(index_name, index_sample_block, MAX_ROWS);
    updateAggregator(aggregator, block);

    const auto granule = takeGranule(aggregator);
    ASSERT_NE(granule, nullptr);
    ASSERT_EQ(granule->set_hyperrectangle.size(), 1);
    expectMeasuredRange("build path", granule->set_hyperrectangle[0], 0, 9);

    MergeTreeIndexGranuleSet restored(index_name, index_sample_block, MAX_ROWS);
    reserializeGranule(*granule, restored);
    ASSERT_EQ(restored.set_hyperrectangle.size(), 1);
    expectMeasuredRange("read path", restored.set_hyperrectangle[0], 0, 9);
}

TEST(MergeTreeIndexSet, GranuleRangeForDynamicColumn)
{
    const String index_name = "idx";
    const Block block{dynamicColumn("value", 0, 10)};
    const Block index_sample_block = block.cloneEmpty();

    MergeTreeIndexAggregatorSet aggregator(index_name, index_sample_block, MAX_ROWS);
    updateAggregator(aggregator, block);

    const auto granule = takeGranule(aggregator);
    ASSERT_NE(granule, nullptr);
    ASSERT_EQ(granule->set_hyperrectangle.size(), 1);
    expectWholeUniverse("build path", granule->set_hyperrectangle[0]);

    MergeTreeIndexGranuleSet restored(index_name, index_sample_block, MAX_ROWS);
    reserializeGranule(*granule, restored);
    ASSERT_EQ(restored.set_hyperrectangle.size(), 1);
    expectWholeUniverse("read path", restored.set_hyperrectangle[0]);
}

TEST(MergeTreeIndexSet, GranuleRangeForNestedDynamicColumn)
{
    const String index_name = "idx";
    const Block block{tupleWithDynamicColumn("value", 0, 10)};
    const Block index_sample_block = block.cloneEmpty();

    MergeTreeIndexAggregatorSet aggregator(index_name, index_sample_block, MAX_ROWS);
    updateAggregator(aggregator, block);

    const auto granule = takeGranule(aggregator);
    ASSERT_NE(granule, nullptr);
    ASSERT_EQ(granule->set_hyperrectangle.size(), 1);
    expectWholeUniverse("build path", granule->set_hyperrectangle[0]);

    MergeTreeIndexGranuleSet restored(index_name, index_sample_block, MAX_ROWS);
    reserializeGranule(*granule, restored);
    ASSERT_EQ(restored.set_hyperrectangle.size(), 1);
    expectWholeUniverse("read path", restored.set_hyperrectangle[0]);
}

TEST(MergeTreeIndexSet, GranuleRangeForNestedVariantColumn)
{
    const String index_name = "idx";
    const Block block{tupleWithVariantColumn("value", 0, 10)};
    const Block index_sample_block = block.cloneEmpty();

    MergeTreeIndexAggregatorSet aggregator(index_name, index_sample_block, MAX_ROWS);
    updateAggregator(aggregator, block);

    const auto granule = takeGranule(aggregator);
    ASSERT_NE(granule, nullptr);
    ASSERT_EQ(granule->set_hyperrectangle.size(), 1);
    expectWholeUniverse("build path", granule->set_hyperrectangle[0]);

    MergeTreeIndexGranuleSet restored(index_name, index_sample_block, MAX_ROWS);
    reserializeGranule(*granule, restored);
    ASSERT_EQ(restored.set_hyperrectangle.size(), 1);
    expectWholeUniverse("read path", restored.set_hyperrectangle[0]);
}

TEST(MergeTreeIndexSet, GranuleRangePerCoordinateAcrossBlocks)
{
    const String index_name = "idx";
    const Block first{dynamicColumn("d", 0, 5), uint64Column("k", 0, 5)};
    const Block second{dynamicColumn("d", 5, 10), uint64Column("k", 5, 10)};
    const Block index_sample_block = first.cloneEmpty();

    MergeTreeIndexAggregatorSet aggregator(index_name, index_sample_block, MAX_ROWS);
    updateAggregator(aggregator, first);
    updateAggregator(aggregator, second);

    const auto granule = takeGranule(aggregator);
    ASSERT_NE(granule, nullptr);
    ASSERT_EQ(granule->set_hyperrectangle.size(), 2);
    expectWholeUniverse("dynamic coordinate", granule->set_hyperrectangle[0]);
    expectMeasuredRange("measurable coordinate", granule->set_hyperrectangle[1], 0, 9);
}
