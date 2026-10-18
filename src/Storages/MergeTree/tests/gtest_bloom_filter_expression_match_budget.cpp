#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
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

enum class TestLambdaCarrier
{
    ColumnFunction,
    FunctionCapture,
};

struct TestLambdaSpec
{
    TestLambdaCarrier carrier;
    bool capture_value;
    UInt64 captured_value;
    UInt64 body_value;
};

struct TestLambdaInResult
{
    bool can_prune;
    bool query_is_column_function;
    bool index_is_column_function;
    bool index_is_function_capture;
};

struct TestMappedLambda
{
    const ActionsDAG::Node * lambda;
    const ActionsDAG::Node * mapped_array;
};

bool isColumnFunctionLambda(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::COLUMN || !node.column)
        return false;

    const auto * column_function = typeid_cast<const ColumnFunction *>(&node.column->getDataColumn());
    return column_function && typeid_cast<const FunctionExpression *>(column_function->getFunction().get());
}

bool isFunctionCaptureLambda(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::FUNCTION
        && node.function_base
        && typeid_cast<const FunctionCapture *>(node.function_base.get());
}

TestMappedLambda addMappedArrayWithLambda(
    ActionsDAG & dag,
    const String & prefix,
    const TestLambdaSpec & spec,
    const ContextPtr & context,
    const DataTypePtr & uint64_type,
    const DataTypePtr & array_type)
{
    ActionsDAG lambda_dag;
    const auto & lambda_argument = lambda_dag.addInput("v", uint64_type);
    const ActionsDAG::Node * captured_input = nullptr;
    if (spec.capture_value)
        captured_input = &lambda_dag.addInput("captured", uint64_type);

    const auto & body_value = lambda_dag.addColumn(
        uint64_type->createColumnConst(0, Field(UInt64(spec.body_value))), uint64_type, "body_value", true);
    const auto & output = lambda_dag.addFunction(
        FunctionFactory::instance().get("plus", context),
        {&lambda_argument, captured_input ? captured_input : &body_value},
        "result");
    lambda_dag.getOutputs().push_back(&output);

    const Names captured_names = spec.capture_value ? Names{"captured"} : Names{};
    const auto lambda_capture = std::make_shared<LambdaCapture>(LambdaCapture{
        .captured_names = captured_names,
        .captured_types = spec.capture_value ? DataTypes{uint64_type} : DataTypes{},
        .lambda_arguments = NamesAndTypesList{{"v", uint64_type}},
        .return_name = output.result_name,
        .return_type = uint64_type,
        .allow_constant_folding = true,
    });
    const auto return_name = output.result_name;

    const auto & array_input = dag.addInput("a", array_type);
    const ActionsDAG::Node * lambda_node = nullptr;
    if (spec.carrier == TestLambdaCarrier::ColumnFunction)
    {
        ColumnsWithTypeAndName captures;
        if (spec.capture_value)
        {
            captures.emplace_back(
                uint64_type->createColumnConst(1, Field(UInt64(spec.captured_value))),
                uint64_type,
                "captured");
        }

        auto expression_actions = std::make_shared<ExpressionActions>(
            std::move(lambda_dag), ExpressionActionsSettings(context));
        FunctionBasePtr function = std::make_shared<FunctionExpression>(lambda_capture, std::move(expression_actions));
        auto column_function = ColumnFunction::create(1, std::move(function), captures);
        auto constant_column_function = ColumnConst::create(std::move(column_function), 0);
        lambda_node = &dag.addColumn(
            std::move(constant_column_function),
            std::make_shared<DataTypeFunction>(DataTypes{uint64_type}, uint64_type),
            prefix + "lambda",
            true);
    }
    else
    {
        auto lambda_resolver = std::make_shared<FunctionCaptureOverloadResolver>(
            std::move(lambda_dag),
            ExpressionActionsSettings(context),
            captured_names,
            NamesAndTypesList{{"v", uint64_type}},
            uint64_type,
            return_name,
            /* allow_constant_folding= */ true);
        lambda_node = &dag.addFunction(lambda_resolver, {}, prefix + "lambda");
    }

    const auto & mapped_array = dag.addFunction(
        FunctionFactory::instance().get("arrayMap", context), {lambda_node, &array_input}, prefix + "mapped");
    return {lambda_node, &mapped_array};
}

TestLambdaInResult canPruneLambdaInWithCarriers(const TestLambdaSpec & query_spec, const TestLambdaSpec & index_spec)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(uint64_type);

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto query_mapping = addMappedArrayWithLambda(
        query_dag, "query_", query_spec, context, uint64_type, array_type);
    const auto index_mapping = addMappedArrayWithLambda(
        index_dag, "index_", index_spec, context, uint64_type, array_type);
    const auto & query_lambda_array = *query_mapping.mapped_array;
    const auto & index_lambda_array = *index_mapping.mapped_array;

    /// Use the query's canonical name for every index output. Mismatched lambdas then reach the
    /// structural matcher instead of declining early because the RPN/header names differ.
    const auto query_name = RPNBuilderTreeNode(&query_lambda_array, context).getColumnName();
    const auto & index_output = index_dag.addAlias(index_lambda_array, query_name);
    index_dag.getOutputs().push_back(&index_output);

    auto set_value = array_type->createColumn();
    set_value->insert(Field(Array{Field(UInt64(4))}));
    ColumnsWithTypeAndName set_columns{{std::move(set_value), array_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node = query_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(
        FunctionFactory::instance().get("in", context), {&query_lambda_array, &set_node}, "predicate");

    Block header(ColumnsWithTypeAndName{{array_type->createColumn(), array_type, query_name}});
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return {
        .can_prune = !condition.alwaysUnknownOrTrue(),
        .query_is_column_function = isColumnFunctionLambda(*query_mapping.lambda),
        .index_is_column_function = isColumnFunctionLambda(*index_mapping.lambda),
        .index_is_function_capture = isFunctionCaptureLambda(*index_mapping.lambda),
    };
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

TEST(MergeTreeIndexBloomFilter, MatchesColumnFunctionLambdaCarrierOnlyWhenEquivalent)
{
    const TestLambdaSpec equal_column_function{
        .carrier = TestLambdaCarrier::ColumnFunction,
        .capture_value = false,
        .captured_value = 0,
        .body_value = 1,
    };
    const TestLambdaSpec captured_column_function{
        .carrier = TestLambdaCarrier::ColumnFunction,
        .capture_value = true,
        .captured_value = 1,
        .body_value = 0,
    };

    const auto equal_result = canPruneLambdaInWithCarriers(equal_column_function, equal_column_function);
    EXPECT_TRUE(equal_result.query_is_column_function);
    EXPECT_TRUE(equal_result.index_is_column_function);
    EXPECT_TRUE(equal_result.can_prune);

    const auto equal_captured_result = canPruneLambdaInWithCarriers(captured_column_function, captured_column_function);
    EXPECT_TRUE(equal_captured_result.query_is_column_function);
    EXPECT_TRUE(equal_captured_result.index_is_column_function);
    EXPECT_TRUE(equal_captured_result.can_prune);

    auto different_capture = captured_column_function;
    different_capture.captured_value = 2;
    const auto capture_result = canPruneLambdaInWithCarriers(captured_column_function, different_capture);
    EXPECT_TRUE(capture_result.query_is_column_function);
    EXPECT_TRUE(capture_result.index_is_column_function);
    EXPECT_FALSE(capture_result.can_prune);

    auto different_body = equal_column_function;
    different_body.body_value = 2;
    const auto body_result = canPruneLambdaInWithCarriers(equal_column_function, different_body);
    EXPECT_TRUE(body_result.query_is_column_function);
    EXPECT_TRUE(body_result.index_is_column_function);
    EXPECT_FALSE(body_result.can_prune);

    auto same_body_function_capture = equal_column_function;
    same_body_function_capture.carrier = TestLambdaCarrier::FunctionCapture;
    const auto mixed_result = canPruneLambdaInWithCarriers(equal_column_function, same_body_function_capture);
    EXPECT_TRUE(mixed_result.query_is_column_function);
    EXPECT_FALSE(mixed_result.index_is_column_function);
    EXPECT_TRUE(mixed_result.index_is_function_capture);
    EXPECT_FALSE(mixed_result.can_prune);
}
