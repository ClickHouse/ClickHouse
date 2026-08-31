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

namespace DB::ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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

    auto values = ColumnUInt32::create();
    values->insert(Field(UInt64(42)));
    const IColumn * value_columns[1] = {values.get()};

    Arena arena;

    /// Build a real, non-empty state for a given version. `operator[]` then spells the field the way
    /// that version prints it and carries a payload serialized at that version, so the only thing that
    /// can reject it on the other column is the version check itself - not a failure to read the bytes.
    auto make_column = [&](size_t version)
    {
        auto column = ColumnAggregateFunction::create(aggregate_function, version);
        column->insertDefault();
        aggregate_function->add(column->getData()[0], value_columns, 0, &arena);
        return column;
    };

    auto column_v0 = make_column(0);
    auto column_v1 = make_column(1);

    const Field field_v0 = (*column_v0)[0];
    const Field field_v1 = (*column_v1)[0];

    ASSERT_FALSE(field_v0.safeGet<AggregateFunctionStateData>().data.empty());
    ASSERT_FALSE(field_v1.safeGet<AggregateFunctionStateData>().data.empty());

    auto expect_rejected = [](ColumnAggregateFunction & column, const Field & field)
    {
        try
        {
            column.insert(field);
            FAIL() << "expected the state to be rejected";
        }
        catch (const Exception & e)
        {
            /// Specifically the type check in `insert`, not a failure while reading the payload.
            EXPECT_EQ(e.code(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    };

    expect_rejected(*column_v1, field_v0);
    expect_rejected(*column_v0, field_v1);

    EXPECT_FALSE(column_v1->tryInsert(field_v0));
    EXPECT_FALSE(column_v0->tryInsert(field_v1));

    /// A state of the column's own version is still accepted, so the guard rejects the version
    /// rather than everything that reaches the slow path.
    EXPECT_NO_THROW(column_v1->insert(field_v1));
    EXPECT_NO_THROW(column_v0->insert(field_v0));

    /// A column built without an explicit version prints its state unversioned, but `insert` passes
    /// that absent version straight to `deserialize`, which reads it as the function's default. The
    /// comparison has to use the same resolution, or a state explicitly spelled as version 0 would be
    /// accepted here and then read at the default version.
    auto column_default = ColumnAggregateFunction::create(aggregate_function);

    AggregateFunctionStateData explicit_v0;
    explicit_v0.name = "AggregateFunction(0, groupBitmap, UInt32)";
    explicit_v0.data = field_v0.safeGet<AggregateFunctionStateData>().data;

    expect_rejected(*column_default, Field(explicit_v0));
    EXPECT_FALSE(column_default->tryInsert(Field(explicit_v0)));

    /// The default version for `groupBitmap` is 1, so a version 1 state is what it does accept.
    EXPECT_NO_THROW(column_default->insert(field_v1));
}
