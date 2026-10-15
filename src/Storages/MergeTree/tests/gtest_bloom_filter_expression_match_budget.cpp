#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/PreparedSets.h>
#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <string>
#include <utility>
#include <vector>

using namespace DB;

namespace
{

const ActionsDAG::Node & addArrayLiteral(
    ActionsDAG & dag,
    const ContextPtr & context,
    const DataTypePtr & uint64_type,
    UInt64 value)
{
    const auto & constant = dag.addColumn(
        uint64_type->createColumnConst(0, Field(value)), uint64_type, "needle", true);
    return dag.addFunction(FunctionFactory::instance().get("array", context), {&constant}, "literal_array");
}

bool canPruneTupleArrayJoinInWithSharedBudget(bool first_component_mismatches)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(uint64_type);

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto & query_input = query_dag.addInput("a", array_type);
    const auto & index_input = index_dag.addInput("a", array_type);

    const UInt64 query_first_value = first_component_mismatches ? 18 : 17;
    const UInt64 query_second_value = first_component_mismatches ? 17 : 18;

    const auto & query_first_literal = addArrayLiteral(query_dag, context, uint64_type, query_first_value);
    const auto & query_second_literal = addArrayLiteral(query_dag, context, uint64_type, query_second_value);
    const auto & query_first_array = query_dag.addFunction(
        FunctionFactory::instance().get("arrayConcat", context),
        {&query_input, &query_first_literal}, "query_first_array");
    const auto & query_second_array = query_dag.addFunction(
        FunctionFactory::instance().get("arrayConcat", context),
        {&query_input, &query_second_literal}, "query_second_array");
    const auto & query_first_join = query_dag.addArrayJoin(query_first_array, "query_first_join");
    const auto & query_second_join = query_dag.addArrayJoin(query_second_array, "query_second_join");
    const auto & query_tuple = query_dag.addFunction(
        FunctionFactory::instance().get("tuple", context), {&query_first_join, &query_second_join}, "query_tuple");

    const auto & index_literal = addArrayLiteral(index_dag, context, uint64_type, 17);
    const auto & index_array = index_dag.addFunction(
        FunctionFactory::instance().get("arrayConcat", context),
        {&index_input, &index_literal}, "index_array");

    /// The matcher budget is 4096. Each output costs at least one unit, so one extra
    /// non-matching output exhausts the first tuple component's shared atom budget.
    constexpr size_t output_count = 4097;
    ColumnsWithTypeAndName header_columns;
    header_columns.reserve(output_count);
    /// Deliberately make the query's RPNBuilder expression name miss the header and use positional fallback.
    for (size_t position = 0; position < output_count; ++position)
    {
        const auto output_name = "index_output_" + std::to_string(position);
        const auto & output = index_dag.addAlias(index_array, output_name);
        index_dag.getOutputs().push_back(&output);
        header_columns.emplace_back(array_type->createColumn(), array_type, output_name);
    }
    Block header(std::move(header_columns));

    auto first_set_value = ColumnUInt64::create();
    first_set_value->insertValue(query_first_value);
    auto second_set_value = ColumnUInt64::create();
    second_set_value->insertValue(query_second_value);
    ColumnsWithTypeAndName set_columns{
        {std::move(first_set_value), uint64_type, "first"},
        {std::move(second_set_value), uint64_type, "second"},
    };
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node = query_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(
        FunctionFactory::instance().get("in", context), {&query_tuple, &set_node}, "predicate");

    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return !condition.alwaysUnknownOrTrue();
}

}

TEST(MergeTreeIndexBloomFilter, SharesExpressionMatchBudgetAcrossTupleINComponents)
{
    /// Without a shared atom budget, component one exhausts its own budget, then component two
    /// matches output zero and contributes a predicate despite having no budget of its own.
    EXPECT_FALSE(canPruneTupleArrayJoinInWithSharedBudget(true));

    /// Exhaustion in a later component must preserve a safe predicate contributed by its sibling.
    EXPECT_TRUE(canPruneTupleArrayJoinInWithSharedBudget(false));
}
