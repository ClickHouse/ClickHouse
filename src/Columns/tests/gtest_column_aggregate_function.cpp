#include <base/types.h>
#include <AggregateFunctions/DDSketch.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
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
/// type name, so a legacy state is spelled exactly like an unversioned one, and the two sides have
/// to resolve an absent version the way `deserialize` will.
TEST(ColumnAggregateFunction, InsertRejectsStateOfAnotherVersion)
{
    tryRegisterAggregateFunctions();

    using namespace DB;

    /// `sumMap` is one of the two genuinely versioned families in the tree: version 0 serializes each
    /// value with its own type and version 1 with the promoted one, so with `UInt8` values the same
    /// state is one byte per value at version 0 and eight at version 1. Most aggregate functions
    /// ignore the version entirely - `groupBitmap` among them, despite sitting in the same file as
    /// the versioned `groupBitmapAnd` - and with such a function this test would assert nothing.
    AggregateFunctionFactory & factory = AggregateFunctionFactory::instance();
    const auto array_of_uint8 = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>());
    DataTypes argument_types = {array_of_uint8, array_of_uint8};
    Array params;
    AggregateFunctionProperties properties;
    auto aggregate_function = factory.get("sumMap", NullsAction::EMPTY, argument_types, params, properties);

    /// The premise of everything below, asserted rather than assumed.
    ASSERT_TRUE(aggregate_function->isVersioned());
    ASSERT_EQ(aggregate_function->getDefaultVersion(), 1u);

    auto keys = ColumnArray::create(ColumnUInt8::create());
    auto values = ColumnArray::create(ColumnUInt8::create());
    keys->insert(Field(Array{Field(UInt64(1))}));
    values->insert(Field(Array{Field(UInt64(5))}));
    const IColumn * argument_columns[2] = {keys.get(), values.get()};

    Arena arena;

    /// Build a real, non-empty state at a given version. `operator[]` then spells the field the way
    /// that version prints it and carries a payload serialized at that version, so the only thing that
    /// can reject it on the other column is the version check itself - not a failure to read the bytes.
    auto make_column = [&](std::optional<size_t> version)
    {
        auto column = ColumnAggregateFunction::create(aggregate_function, version);
        column->insertDefault();
        aggregate_function->add(column->getData()[0], argument_columns, 0, &arena);
        return column;
    };

    auto column_v0 = make_column(0);
    auto column_v1 = make_column(1);

    const Field field_v0 = (*column_v0)[0];
    const Field field_v1 = (*column_v1)[0];

    const auto & data_v0 = field_v0.safeGet<AggregateFunctionStateData>().data;
    const auto & data_v1 = field_v1.safeGet<AggregateFunctionStateData>().data;

    ASSERT_FALSE(data_v0.empty());
    ASSERT_FALSE(data_v1.empty());
    /// The two versions differ in the bytes and not only in the type name, so accepting one for the
    /// other would really misread the payload.
    ASSERT_NE(data_v0, data_v1);

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

    /// A state of the column's own version is still accepted, so the guard rejects the version rather
    /// than everything that reaches the slow path.
    EXPECT_NO_THROW(column_v1->insert(field_v1));
    EXPECT_NO_THROW(column_v0->insert(field_v0));

    /// A column built without an explicit version hands that absent version straight to `deserialize`,
    /// which reads it as the function's default. The comparison has to use the same resolution: a state
    /// explicitly spelled as version 0 is rejected, and one spelled as the default version accepted,
    /// even though the column itself prints no version at all.
    auto column_default = ColumnAggregateFunction::create(aggregate_function);

    AggregateFunctionStateData explicit_v0;
    explicit_v0.name = "AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8))";
    explicit_v0.data = data_v0;

    /// Neither `getTypeString` nor `DataTypeAggregateFunction::getName` ever prints a zero version, so
    /// this spelling only reaches us from metadata written by an older server. Pin it down: if the name
    /// ever stopped parsing, the slow path would bail out early and the rejection below would pass for
    /// the wrong reason.
    const auto explicit_v0_type = DataTypeFactory::instance().tryGet(explicit_v0.name);
    ASSERT_TRUE(explicit_v0_type != nullptr);
    ASSERT_EQ(
        typeid_cast<const DataTypeAggregateFunction &>(*explicit_v0_type).getVersionIfExplicit(),
        std::optional<size_t>(0));

    expect_rejected(*column_default, Field(explicit_v0));
    EXPECT_FALSE(column_default->tryInsert(Field(explicit_v0)));

    EXPECT_NO_THROW(column_default->insert(field_v1));
}
