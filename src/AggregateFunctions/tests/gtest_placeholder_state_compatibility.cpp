#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/tests/gtest_global_register.h>
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
