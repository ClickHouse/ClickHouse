#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMiscellaneous.h>
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
    const DataTypePtr & array_type,
    UInt64 value)
{
    return dag.addColumn(
        array_type->createColumnConst(0, Field(Array{Field(value)})), array_type, "literal_array", true);
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

    const auto & query_first_literal = addArrayLiteral(query_dag, array_type, query_first_value);
    const auto & query_second_literal = addArrayLiteral(query_dag, array_type, query_second_value);
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

    const auto & index_literal = addArrayLiteral(index_dag, array_type, 17);
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

bool canPruneDecimalToStringIn(const DataTypePtr & query_decimal_type, const DataTypePtr & index_decimal_type)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto string_type = std::make_shared<DataTypeString>();

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto & query_input = query_dag.addInput("x", query_decimal_type);
    const auto & index_input = index_dag.addInput("x", index_decimal_type);
    const auto & query_to_string = query_dag.addFunction(
        FunctionFactory::instance().get("toString", context), {&query_input}, "query_to_string");
    const auto & index_to_string = index_dag.addFunction(
        FunctionFactory::instance().get("toString", context), {&index_input}, "index_to_string");
    /// DirectIN resolves the indexed expression by the canonical function column name.
    const auto & indexed_output = index_dag.addAlias(index_to_string, "toString(x)");
    index_dag.getOutputs().push_back(&indexed_output);

    ColumnsWithTypeAndName header_columns{{string_type->createColumn(), string_type, "toString(x)"}};
    Block header(std::move(header_columns));

    auto set_value = ColumnString::create();
    set_value->insertData("3", 1);
    ColumnsWithTypeAndName set_columns{{std::move(set_value), string_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node = query_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(
        FunctionFactory::instance().get("in", context), {&query_to_string, &set_node}, "predicate");

    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return !condition.alwaysUnknownOrTrue();
}

bool canPruneLambdaInWithFunctionCount(size_t function_count)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(uint64_type);

    const auto add_mapped_array = [&](ActionsDAG & dag, const String & prefix) -> const ActionsDAG::Node &
    {
        ActionsDAG lambda_dag;
        const auto & lambda_argument = lambda_dag.addInput("v", uint64_type);
        const auto & one = lambda_dag.addColumn(
            uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "one", true);
        const auto plus = FunctionFactory::instance().get("plus", context);

        /// Keep the returned expression small while storing more body DAG nodes than the matcher's
        /// shared work budget can admit. Every node must be budgeted, including nodes not in outputs.
        const ActionsDAG::Node * output = nullptr;
        for (size_t i = 0; i < function_count; ++i)
            output = &lambda_dag.addFunction(plus, {&lambda_argument, &one}, "unused_" + std::to_string(i));
        lambda_dag.getOutputs().clear();
        lambda_dag.getOutputs().push_back(output);
        const auto return_name = output->result_name;

        const auto lambda_capture = std::make_shared<FunctionCaptureOverloadResolver>(
            std::move(lambda_dag),
            ExpressionActionsSettings(context),
            Names{},
            NamesAndTypesList{{"v", uint64_type}},
            uint64_type,
            return_name,
            /* allow_constant_folding= */ true);

        const auto & array_input = dag.addInput("a", array_type);
        const auto & lambda = dag.addFunction(lambda_capture, {}, prefix + "lambda");
        return dag.addFunction(
            FunctionFactory::instance().get("arrayMap", context), {&lambda, &array_input}, prefix + "mapped");
    };

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto & query_mapped = add_mapped_array(query_dag, "query_");
    const auto & index_mapped = add_mapped_array(index_dag, "index_");
    const auto index_name = RPNBuilderTreeNode(&index_mapped, context).getColumnName();
    const auto & index_output = index_dag.addAlias(index_mapped, index_name);
    index_dag.getOutputs().clear();
    index_dag.getOutputs().push_back(&index_output);

    auto set_value = array_type->createColumn();
    set_value->insert(Field(Array{Field(UInt64(4))}));
    ColumnsWithTypeAndName set_columns{{std::move(set_value), array_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node = query_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(
        FunctionFactory::instance().get("in", context), {&query_mapped, &set_node}, "predicate");

    Block header(ColumnsWithTypeAndName{{array_type->createColumn(), array_type, index_name}});
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

TEST(MergeTreeIndexBloomFilter, MatchesDecimalParametersForToStringIN)
{
    EXPECT_TRUE(canPruneDecimalToStringIn(
        std::make_shared<DataTypeDecimal64>(10, 2), std::make_shared<DataTypeDecimal64>(10, 2)));
    EXPECT_FALSE(canPruneDecimalToStringIn(
        std::make_shared<DataTypeDecimal64>(18, 2), std::make_shared<DataTypeDecimal64>(10, 2)));
    EXPECT_FALSE(canPruneDecimalToStringIn(
        std::make_shared<DataTypeDecimal64>(10, 3), std::make_shared<DataTypeDecimal64>(10, 2)));
}

TEST(MergeTreeIndexBloomFilter, DeclinesLambdaBodyWhenSharedExpressionMatchBudgetIsExceeded)
{
    EXPECT_TRUE(canPruneLambdaInWithFunctionCount(1));
    EXPECT_FALSE(canPruneLambdaInWithFunctionCount(4090));
}
