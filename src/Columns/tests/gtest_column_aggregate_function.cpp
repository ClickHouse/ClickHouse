#include <base/types.h>
#include <AggregateFunctions/DDSketch.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <Common/Arena.h>
#include <Common/Base64.h>
#include <Common/FailPoint.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

namespace DB::FailPoints
{
    extern const char column_aggregate_function_ensureOwnership_exception[];
}

TEST(ColumnAggregateFunction, EnsureOwnershipExceptionLeavesCorruptedState)
{
    tryRegisterAggregateFunctions();

    using namespace DB;

    // Create the aggregate function quantileDD with relative accuracy 0.01
    AggregateFunctionFactory & factory = AggregateFunctionFactory::instance();
    DataTypes argument_types = {std::make_shared<DataTypeFloat64>()};
    Array params = {Field(0.01), Field(0.5)};
    AggregateFunctionProperties properties;
    auto aggregate_function = factory.get("quantileDD", NullsAction::EMPTY, argument_types, params, properties);

    // Create a source column with some data
    auto src_column = ColumnAggregateFunction::create(aggregate_function);
    Arena arena_src;
    auto data_column = ColumnFloat64::create();
    data_column->insert(Field(1.0));
    data_column->insert(Field(2.0));
    data_column->insert(Field(3.0));
    const IColumn * columns[1] = {data_column.get()};

    for (size_t i = 0; i < 3; ++i)
    {
        src_column->insertDefault();
        aggregate_function->add(src_column->getData()[i], columns, i, &arena_src);
    }

    // Create a view column from the source - this sets src pointer
    auto view_column = src_column->cloneEmpty();
    view_column->insertRangeFrom(*src_column, 0, 3);

    // Enable failpoint that will trigger an exception during ensureOwnership
    // This will happen after at least one state is created and destroyed
    FailPointInjection::enableFailPoint(FailPoints::column_aggregate_function_ensureOwnership_exception);

    // Try to insert - this will call ensureOwnership() which will throw
    // After the exception, previously, data[] points to destroyed memory where mapping == nullptr
    ASSERT_THROW({
        view_column->insertDefault();
    }, Exception);

    // Disable failpoint
    FailPointInjection::disableFailPoint(FailPoints::column_aggregate_function_ensureOwnership_exception);

    /// Previously leads to a crash
    view_column->insertDefault();
}

/// `insert` accepts a field whose state type is spelled differently but describes the same state,
/// which is what lets a `LowCardinality` argument survive the round trip through `getDefault`.
/// It must not accept a state of a different serialization version: version 0 is not printed in a
/// type name, so a legacy state is spelled exactly like an unversioned one, and resolving that
/// absent version to the function's default would let a version 0 payload be read as version 1.
TEST(ColumnAggregateFunction, InsertRejectsStateOfAnotherVersion)
{
    tryRegisterAggregateFunctions();

    using namespace DB;

    /// `groupBitmap` is versioned: its default version is 1.
    AggregateFunctionFactory & factory = AggregateFunctionFactory::instance();
    DataTypes argument_types = {std::make_shared<DataTypeUInt32>()};
    Array params;
    AggregateFunctionProperties properties;
    auto aggregate_function = factory.get("groupBitmap", NullsAction::EMPTY, argument_types, params, properties);

    auto column_v1 = ColumnAggregateFunction::create(aggregate_function, 1);
    auto column_v0 = ColumnAggregateFunction::create(aggregate_function, 0);

    /// A version 1 column prints its version, a version 0 column does not.
    AggregateFunctionStateData state_v1;
    state_v1.name = "AggregateFunction(1, groupBitmap, UInt32)";

    AggregateFunctionStateData state_v0;
    state_v0.name = "AggregateFunction(groupBitmap, UInt32)";

    /// The version is checked before the payload is touched, so the states may stay empty.
    EXPECT_THROW(column_v1->insert(Field(state_v0)), Exception);
    EXPECT_THROW(column_v0->insert(Field(state_v1)), Exception);

    EXPECT_FALSE(column_v1->tryInsert(Field(state_v0)));
    EXPECT_FALSE(column_v0->tryInsert(Field(state_v1)));
}
