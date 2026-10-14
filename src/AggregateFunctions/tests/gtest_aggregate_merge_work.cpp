#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionNull.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/AlignedBuffer.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;

namespace
{

AggregateFunctionPtr getFunction(const String & name, const DataTypes & arguments)
{
    tryRegisterAggregateFunctions();
    AggregateFunctionProperties properties;
    return AggregateFunctionFactory::instance().get(name, NullsAction::EMPTY, arguments, {}, properties);
}

ColumnPtr values()
{
    auto column = ColumnUInt64::create();
    for (UInt64 value : {1, 2, 2, 3})
        column->insertValue(value);
    return column;
}

/// A work estimate counts retained state elements, including when a combinator changes how input rows arrive.
void checkWork(
    const IAggregateFunction & function,
    const IAggregateFunction & populate,
    const Columns & columns,
    size_t expected_work,
    bool expected_prepare = true)
{
    AlignedBuffer place(function.sizeOfData(), function.alignOfData());
    function.create(place.data());
    ColumnRawPtrs arguments;
    for (const auto & column : columns)
        arguments.push_back(column.get());
    for (size_t row = 0; row < columns.front()->size(); ++row)
        populate.add(place.data(), arguments.data(), row, nullptr);
    EXPECT_EQ(function.getEstimatedMergeWork(place.data()), expected_work);
    EXPECT_EQ(function.isParallelizeMergePrepareNeeded(), expected_prepare);
    function.destroy(place.data());
}

}

TEST(AggregateMergeWork, WrappersWithSameStateRepresentation)
{
    const auto value_type = std::make_shared<DataTypeUInt64>();
    const auto condition_type = std::make_shared<DataTypeUInt8>();
    const auto state = getFunction("uniqExactState", {value_type});
    const std::vector<std::pair<String, DataTypes>> cases{
        {"uniqExactIf", {value_type, condition_type}},
        {"uniqExactArray", {std::make_shared<DataTypeArray>(value_type)}},
        {"uniqExactState", {value_type}},
        {"uniqExactMerge", {state->getResultType()}},
        {"uniqExactArrayIfState", {std::make_shared<DataTypeArray>(value_type), condition_type}},
    };
    const bool parallelizable = getFunction("uniqExact", {value_type})->isAbleToParallelizeMerge();
    for (const auto & [name, arguments] : cases)
    {
        SCOPED_TRACE(name);
        const auto function = getFunction(name, arguments);
        EXPECT_EQ(function->isAbleToParallelizeMerge(), parallelizable);
        /// These wrappers retain the base function's state layout, so the base can populate their states directly.
        checkWork(*function, function->getBaseAggregateFunctionWithSameStateRepresentation(), {values()}, 3);
        /// An empty state has no merge work, but still retains the nested function's preparation requirement.
        checkWork(*function, function->getBaseAggregateFunctionWithSameStateRepresentation(), {ColumnUInt64::create()}, 0);
    }
}

TEST(AggregateMergeWork, OrFillKeepsNestedMergeHooks)
{
    /// `-OrNull` and `-OrDefault` keep a flag after the nested state, so they populate their own states. They report
    /// the nested function's merge work, preparation requirement and parallel merge.
    const auto value_type = std::make_shared<DataTypeUInt64>();
    const bool parallelizable = getFunction("uniqExact", {value_type})->isAbleToParallelizeMerge();
    for (const auto * name : {"uniqExactOrNull", "uniqExactOrDefault"})
    {
        SCOPED_TRACE(name);
        const auto function = getFunction(name, {value_type});
        EXPECT_EQ(function->isAbleToParallelizeMerge(), parallelizable);
        checkWork(*function, *function, {values()}, 3);
        checkWork(*function, *function, {ColumnUInt64::create()}, 0);
    }
}

TEST(AggregateMergeWork, VariadicAndTupleArguments)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto variadic = getFunction("uniqExact", {type, type});
    checkWork(*variadic, *variadic, {values(), values()}, 3);
    const auto tuple = getFunction("uniqExact", {std::make_shared<DataTypeTuple>(DataTypes{type, type})});
    checkWork(*tuple, *tuple, {ColumnTuple::create(Columns{values(), values()})}, 3);
}

TEST(AggregateMergeWork, TupleCombinatorStateOffsets)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto nullable_type = std::make_shared<DataTypeNullable>(type);
    const auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{type, nullable_type});
    auto nullable = nullable_type->createColumn();
    nullable->insert(UInt64{1});
    nullable->insertDefault();
    nullable->insert(UInt64{1});
    nullable->insert(UInt64{2});
    const Columns columns{ColumnTuple::create(Columns{values(), std::move(nullable)})};

    /// Tuple elements retain separate states, so their merge work adds even when their values overlap.
    const auto exact = getFunction("uniqExactTuple", {tuple_type});
    checkWork(*exact, *exact, columns, 5);
    checkWork(*exact, *exact, {tuple_type->createColumn()}, 0);

    const auto approximate = getFunction("uniqTuple", {tuple_type});
    checkWork(*approximate, *approximate, columns, 0, false);
}

TEST(AggregateMergeWork, NullableStateOffsets)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto nullable_type = std::make_shared<DataTypeNullable>(type);
    auto column = nullable_type->createColumn();
    for (UInt64 value : {1, 2, 2, 3})
        column->insert(value);
    column->insertDefault();
    const Columns columns{std::move(column)};
    const auto nested = getFunction("uniqExact", {type});

    /// Both state layouts are valid: a nullable result adds an aligned presence flag before the nested state.
    AggregateFunctionNullUnary<false, false> no_prefix(nested, {nullable_type}, {});
    checkWork(no_prefix, no_prefix, columns, 3);
    AggregateFunctionNullUnary<true, true> with_prefix(nested, {nullable_type}, {});
    checkWork(with_prefix, with_prefix, columns, 3);
}

TEST(AggregateMergeWork, FunctionsWithoutWorkEstimates)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto function = getFunction("uniqIf", {type, std::make_shared<DataTypeUInt8>()});
    checkWork(*function, function->getBaseAggregateFunctionWithSameStateRepresentation(), {values()}, 0, false);
}

TEST(AggregateMergeWork, RetainedStateCostThroughWrappers)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto nullable_type = std::make_shared<DataTypeNullable>(type);
    const auto condition_type = std::make_shared<DataTypeUInt8>();
    const size_t bytes = getFunction("uniqExact", {type})->getStateBytesPerDistinctInput();
    ASSERT_GT(bytes, 0);

    /// These wrappers apply the nested function at most once per row, so the distinct argument tuples the adaptive
    /// sampler hashes bound its distinct inputs from above.
    EXPECT_EQ(getFunction("uniqExactIf", {type, condition_type})->getStateBytesPerDistinctInput(), bytes);
    EXPECT_EQ(getFunction("uniqExactState", {type})->getStateBytesPerDistinctInput(), bytes);
    EXPECT_EQ(getFunction("uniqExactOrNull", {type})->getStateBytesPerDistinctInput(), bytes);
    EXPECT_EQ(getFunction("uniqExactOrDefault", {type})->getStateBytesPerDistinctInput(), bytes);
    EXPECT_EQ(getFunction("uniqExact", {nullable_type})->getStateBytesPerDistinctInput(), bytes);
    EXPECT_EQ(
        getFunction("uniqExactTuple", {std::make_shared<DataTypeTuple>(DataTypes{type, type})})->getStateBytesPerDistinctInput(),
        2 * bytes);

    /// An array argument adds several values per row, which a distinct array does not bound.
    EXPECT_EQ(getFunction("uniqExactArray", {std::make_shared<DataTypeArray>(type)})->getStateBytesPerDistinctInput(), 0);
    EXPECT_EQ(getFunction("uniqIf", {type, condition_type})->getStateBytesPerDistinctInput(), 0);
}
