#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;

static AggregateFunctionPtr resolve(const String & name, const DataTypes & arguments, double level)
{
    Array parameters;
    parameters.push_back(level);
    AggregateFunctionProperties properties;
    return AggregateFunctionFactory::instance().get(name, NullsAction::EMPTY, arguments, parameters, properties);
}

static AggregateFunctionPtr resolveNoParams(const String & name, const DataTypes & arguments)
{
    AggregateFunctionProperties properties;
    return AggregateFunctionFactory::instance().get(name, NullsAction::EMPTY, arguments, {}, properties);
}

/// Only-null arguments collapse an aggregate function to a `nothing*` placeholder that keeps the
/// original finalization parameters, while its state is an empty, parameter-independent
/// serialization. `-Merge` validates states through `haveSameStateRepresentation`, whose default
/// implementation compares normalized state types, so placeholders created with different
/// parameters must report the same state representation, like the real functions they replace do.
TEST(AggregateFunctionNothingPlaceholder, StateRepresentationIgnoresParameters)
{
    tryRegisterAggregateFunctions();

    DataTypes only_null_args{std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>())};
    auto placeholder_a = resolve("quantileExact", only_null_args, 0.5);
    auto placeholder_b = resolve("quantileExact", only_null_args, 0.9);

    EXPECT_TRUE(placeholder_a->haveSameStateRepresentation(*placeholder_b));
    EXPECT_TRUE(placeholder_b->haveSameStateRepresentation(*placeholder_a));

    /// The same holds element-wise for the -Tuple combinator with an only-null element.
    DataTypes tuple_args{std::make_shared<DataTypeTuple>(
        DataTypes{only_null_args[0], std::make_shared<DataTypeFloat64>()})};
    auto tuple_a = resolve("quantileExactTuple", tuple_args, 0.5);
    auto tuple_b = resolve("quantileExactTuple", tuple_args, 0.9);

    EXPECT_TRUE(tuple_a->haveSameStateRepresentation(*tuple_b));
    EXPECT_TRUE(tuple_b->haveSameStateRepresentation(*tuple_a));
}

TEST(AggregateFunctionStateCompatibility, NullableTupleReturningIfDoesNotMatchBareState)
{
    tryRegisterAggregateFunctions();

    const auto float64_type = std::make_shared<DataTypeFloat64>();
    const auto nullable_float64_type = std::make_shared<DataTypeNullable>(float64_type);
    const auto uint8_type = std::make_shared<DataTypeUInt8>();

    const auto nullable_bare = resolveNoParams(
        "simpleLinearRegression",
        DataTypes{nullable_float64_type, nullable_float64_type});
    const auto nullable_if = resolveNoParams(
        "simpleLinearRegressionIf",
        DataTypes{nullable_float64_type, nullable_float64_type, uint8_type});

    EXPECT_FALSE(nullable_bare->haveSameStateRepresentation(*nullable_if));
    EXPECT_FALSE(nullable_if->haveSameStateRepresentation(*nullable_bare));
    EXPECT_FALSE(nullable_bare->getStateType()->equals(*nullable_if->getStateType()));

    const auto non_nullable_bare = resolveNoParams(
        "simpleLinearRegression",
        DataTypes{float64_type, float64_type});
    const auto non_nullable_if = resolveNoParams(
        "simpleLinearRegressionIf",
        DataTypes{float64_type, float64_type, uint8_type});

    EXPECT_TRUE(non_nullable_bare->haveSameStateRepresentation(*non_nullable_if));
    EXPECT_TRUE(non_nullable_if->haveSameStateRepresentation(*non_nullable_bare));
}

TEST(AggregateFunctionStateCompatibility, SerializedNullableFlagAffectsStateCompatibility)
{
    tryRegisterAggregateFunctions();

    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto nullable_uint8_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());

    const auto bare = resolveNoParams("uniq", DataTypes{uint64_type});
    const auto if_nullable_condition = resolveNoParams("uniqIf", DataTypes{uint64_type, nullable_uint8_type});

    EXPECT_EQ(bare->sizeOfData(), if_nullable_condition->sizeOfData());
    EXPECT_EQ(bare->alignOfData(), if_nullable_condition->alignOfData());
    EXPECT_FALSE(bare->haveSameStateRepresentation(*if_nullable_condition));
    EXPECT_FALSE(if_nullable_condition->haveSameStateRepresentation(*bare));
    EXPECT_FALSE(bare->getStateType()->equals(*if_nullable_condition->getStateType()));
}
