#include <base/types.h>
#include <AggregateFunctions/DDSketch.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <Common/Arena.h>
#include <Common/Base64.h>
#include <Common/assert_cast.h>
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

/// `createView` and the copy constructor used to drop the aggregate-state `version`, so a column
/// produced by `permute`, `filter`, `cloneResized`, ... serialized its states with the default
/// format instead of the one named by its type. `type_string` was even lost entirely in copies.
TEST(ColumnAggregateFunction, ViewsAndCopiesPreserveVersion)
{
    tryRegisterAggregateFunctions();

    using namespace DB;

    AggregateFunctionFactory & factory = AggregateFunctionFactory::instance();
    DataTypes argument_types
        = {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>()),
           std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>())};
    AggregateFunctionProperties properties;
    /// `sumMap` is versioned: version 0 and version 1 serialize the values differently.
    auto aggregate_function = factory.get("sumMap", NullsAction::EMPTY, argument_types, {}, properties);
    ASSERT_EQ(aggregate_function->getDefaultVersion(), size_t{1});

    /// A non-zero version is the one that shows up in the type name.
    const size_t version = 1;
    auto column = ColumnAggregateFunction::create(aggregate_function, version);
    column->insertDefault();
    column->insertDefault();

    const String expected_type_string = (*column)[0].safeGet<AggregateFunctionStateData>().name;
    /// The version is part of the type name, which is what makes it observable here.
    ASSERT_TRUE(expected_type_string.starts_with("AggregateFunction(1, sumMap")) << expected_type_string;

    IColumn::Permutation permutation = {1, 0};
    auto view = column->permute(permutation, 0);
    EXPECT_EQ((*view)[0].safeGet<AggregateFunctionStateData>().name, expected_type_string);

    auto copy = ColumnAggregateFunction::create(assert_cast<const ColumnAggregateFunction &>(*column));
    EXPECT_EQ((*copy)[0].safeGet<AggregateFunctionStateData>().name, expected_type_string);
}
