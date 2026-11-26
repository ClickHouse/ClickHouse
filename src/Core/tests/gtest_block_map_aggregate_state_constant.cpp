#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace
{

/// A single-row constant `Map(String, state_type)` holding one entry with the given key and a
/// default (empty) aggregate state as its value.
ColumnWithTypeAndName makeConstantMapOfAggregateState(const DataTypePtr & state_type, const String & key)
{
    auto state_column = state_type->createColumn();
    state_column->insertDefault();

    Field state_field;
    state_column->get(0, state_field);

    auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), state_type);
    auto map_column = map_type->createColumn();
    map_column->insert(Map{Tuple{key, state_field}});

    return ColumnWithTypeAndName{ColumnConst::create(std::move(map_column), 1), map_type, "m"};
}

Block makeBlock(ColumnWithTypeAndName column)
{
    return Block{std::move(column)};
}

}

/// Aggregate states whose functions have the same state representation are compatible, and for
/// constants the comparison of their values is relaxed to the serialized state, because the
/// function names are allowed to differ. This relaxation has to work through plain containers such
/// as `Map`: the type of every value a `Map` holds is fixed by the declared `Map(K, V)`, so a `Map`
/// cannot hide which type a nested value has (unlike `Variant`, `Dynamic` and `JSON`).
GTEST_TEST(BlockStructure, ConstantMapRelaxesTheNestedAggregateState)
{
    tryRegisterAggregateFunctions();

    auto quantile_type = DataTypeFactory::instance().get("AggregateFunction(quantile(0.5), UInt8)");
    auto quantiles_type = DataTypeFactory::instance().get("AggregateFunction(quantiles(0.9), UInt8)");

    Block quantile = makeBlock(makeConstantMapOfAggregateState(quantile_type, "k"));
    Block quantiles = makeBlock(makeConstantMapOfAggregateState(quantiles_type, "k"));

    EXPECT_TRUE(blocksHaveEqualStructure(quantile, quantiles));
    EXPECT_TRUE(blocksHaveEqualStructure(quantiles, quantile));
}

/// Everything else inside the constant `Map` is still compared strictly: a compatible aggregate
/// state next to a differing key is a different constant.
GTEST_TEST(BlockStructure, ConstantMapKeepsComparingTheOtherValues)
{
    tryRegisterAggregateFunctions();

    auto quantile_type = DataTypeFactory::instance().get("AggregateFunction(quantile(0.5), UInt8)");
    auto quantiles_type = DataTypeFactory::instance().get("AggregateFunction(quantiles(0.9), UInt8)");

    Block quantile = makeBlock(makeConstantMapOfAggregateState(quantile_type, "k"));
    Block quantiles = makeBlock(makeConstantMapOfAggregateState(quantiles_type, "other"));

    EXPECT_FALSE(blocksHaveEqualStructure(quantile, quantiles));
    EXPECT_FALSE(blocksHaveEqualStructure(quantiles, quantile));
}
