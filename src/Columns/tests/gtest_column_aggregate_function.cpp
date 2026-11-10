#include <base/types.h>
#include <AggregateFunctions/DDSketch.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
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

/// The name inside a state `Field` is compared against the state type's name, so the two must spell
/// a parameter the same way. A `Decimal` level is where the two printers can diverge: the type name
/// suffixes it with `::Type`, and an unsuffixed copy reparses as a `String` the level check rejects.
TEST(ColumnAggregateFunction, StateFieldNameMatchesStateTypeName)
{
    tryRegisterAggregateFunctions();

    using namespace DB;

    DataTypes argument_types = {std::make_shared<DataTypeFloat64>()};
    Array params = {DecimalField<Decimal32>(5, 1)};
    AggregateFunctionProperties properties;
    auto aggregate_function
        = AggregateFunctionFactory::instance().get("quantile", NullsAction::EMPTY, argument_types, params, properties);

    auto column = ColumnAggregateFunction::create(aggregate_function);
    column->insertDefault();

    Field field;
    column->get(0, field);

    const auto state_type = std::make_shared<DataTypeAggregateFunction>(aggregate_function, argument_types, params);
    ASSERT_EQ(field.safeGet<AggregateFunctionStateData>().name, state_type->getName());
    ASSERT_EQ(state_type->getName(), "AggregateFunction(quantile('0.5'::Decimal32(1)), Float64)");
}
