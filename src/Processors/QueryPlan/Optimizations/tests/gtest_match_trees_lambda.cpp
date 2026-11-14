#include <gtest/gtest.h>

#include "config.h"

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <QueryPipeline/SizeLimits.h>

#include <functional>

using namespace DB;

namespace
{

const DataTypePtr u64 = std::make_shared<DataTypeUInt64>();
const DataTypePtr u64_array = std::make_shared<DataTypeArray>(u64);

using BodyBuilder = std::function<const ActionsDAG::Node &(ActionsDAG &, const ActionsDAG::Node & arg, const ActionsDAG::Node & captured)>;

const ActionsDAG::Node & addCall(ActionsDAG & dag, const String & name, ActionsDAG::NodeRawConstPtrs children, const String & result_name)
{
    return dag.addFunction(FunctionFactory::instance().get(name, getContext().context), std::move(children), result_name);
}

BodyBuilder binaryBody(const String & name)
{
    return [name](ActionsDAG & dag, const ActionsDAG::Node & arg, const ActionsDAG::Node & captured) -> const ActionsDAG::Node &
    { return addCall(dag, name, {&arg, &captured}, "body"); };
}

ExpressionActionsSettings lambdaSettings(bool compile)
{
    ExpressionActionsSettings settings;
    settings.can_compile_expressions = compile;
    settings.compile_expressions = compile ? CompileExpressions::yes : CompileExpressions::no;
    settings.min_count_to_compile_expression = 0;
    return settings;
}

/// `arg -> body(arg, captured)`.
FunctionOverloadResolverPtr makeLambda(
    const String & captured_name,
    const DataTypePtr & captured_type,
    const String & arg_name,
    const DataTypePtr & arg_type,
    const BodyBuilder & body,
    bool compile = false)
{
    ActionsDAG lambda_dag;
    const auto & captured = lambda_dag.addInput(captured_name, captured_type);
    const auto & arg = lambda_dag.addInput(arg_name, arg_type);
    const auto & result = body(lambda_dag, arg, captured);
    auto result_type = result.result_type;
    auto result_name = result.result_name;
    lambda_dag.getOutputs() = {&result};

    return std::make_shared<FunctionCaptureOverloadResolver>(
        std::move(lambda_dag),
        lambdaSettings(compile),
        Names{captured_name},
        NamesAndTypesList{{arg_name, arg_type}},
        result_type,
        result_name,
        /*allow_constant_folding=*/true);
}

ColumnConstPtr constUInt64(UInt64 value)
{
    return ColumnConst::create(ColumnUInt64::create(1, value), 1);
}

ColumnConstPtr constSet(std::vector<UInt64> values)
{
    auto column = ColumnUInt64::create();
    for (auto value : values)
        column->insert(value);
    ColumnsWithTypeAndName block{{std::move(column), u64, "s"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(FutureSet::Hash{}, nullptr, std::move(block), false, SizeLimits{});
    return ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 1);
}

struct MappedDag
{
    ActionsDAG dag;
    const ActionsDAG::Node * lambda = nullptr;
    const ActionsDAG::Node * mapped = nullptr;
};

void finish(MappedDag & result, const ActionsDAG::Node & lambda, const ActionsDAG::Node & array)
{
    result.lambda = &lambda;
    result.mapped = &addCall(result.dag, "arrayMap", {&lambda, &array}, "mapped");
    result.dag.getOutputs() = {result.mapped};
}

/// `arrayMap(lambda(<constant>), arr)`: the lambda is folded into a constant `ColumnFunction`.
std::unique_ptr<MappedDag> mapWithFoldedLambda(
    const FunctionOverloadResolverPtr & lambda, ColumnConstPtr constant, const DataTypePtr & constant_type, const String & constant_name)
{
    auto result = std::make_unique<MappedDag>();
    const auto & array = result->dag.addInput("arr", u64_array);
    const auto & captured = result->dag.addColumn(std::move(constant), constant_type, constant_name);
    finish(*result, result->dag.addFunction(lambda, {&captured}, "lambda"), array);
    return result;
}

std::unique_ptr<MappedDag> mapWithFoldedLambda(const FunctionOverloadResolverPtr & lambda, const String & constant_name = "c")
{
    return mapWithFoldedLambda(lambda, constUInt64(10), u64, constant_name);
}

const FunctionExpression * foldedExpression(const ActionsDAG::Node & node)
{
    if (!node.column)
        return nullptr;
    const auto * column_function = typeid_cast<const ColumnFunction *>(&node.column->getDataColumn());
    if (!column_function)
        return nullptr;
    return typeid_cast<const FunctionExpression *>(column_function->getFunction().get());
}

bool foldedValuesEqual(const MappedDag & lhs, const MappedDag & rhs)
{
    return (*lhs.lambda->column)[0] == (*rhs.lambda->column)[0];
}

/// The node of `inner` that `matchTrees` maps the `arrayMap` of `outer` onto, without monotonicity.
const ActionsDAG::Node * matchOf(const ActionsDAG::NodeRawConstPtrs & inner_outputs, const ActionsDAG & outer_dag, const ActionsDAG::Node * outer)
{
    auto matches = matchTrees(inner_outputs, outer_dag, /*check_monotonicity=*/false, /*max_size_for_sets_from_tuple_to_compare=*/1000);
    auto it = matches.find(outer);
    if (it == matches.end() || it->second.monotonicity)
        return nullptr;
    return it->second.node;
}

const ActionsDAG::Node * matchOf(const MappedDag & inner, const MappedDag & outer)
{
    return matchOf(inner.dag.getOutputs(), outer.dag, outer.mapped);
}

}

TEST(MatchTreesLambda, FoldedLambdasWithDifferentBodiesDoNotMatch)
{
    tryRegisterFunctions();

    auto inner = mapWithFoldedLambda(makeLambda("c", u64, "x", u64, binaryBody("plus")));
    auto outer = mapWithFoldedLambda(makeLambda("c", u64, "x", u64, binaryBody("multiply")));

    ASSERT_NE(foldedExpression(*inner->lambda), nullptr);
    ASSERT_NE(foldedExpression(*outer->lambda), nullptr);
    ASSERT_TRUE(foldedValuesEqual(*inner, *outer)) << "the folded values must not tell the two lambdas apart";

    EXPECT_EQ(matchOf(*inner, *outer), nullptr);
}

TEST(MatchTreesLambda, FoldedLambdasWithSameBodyMatch)
{
    tryRegisterFunctions();

    /// The captured constant is named differently inside the two lambdas.
    auto inner = mapWithFoldedLambda(makeLambda("c", u64, "x", u64, binaryBody("plus")), "c");
    auto outer = mapWithFoldedLambda(makeLambda("10_UInt8", u64, "x", u64, binaryBody("plus")), "10_UInt8");

    ASSERT_NE(foldedExpression(*inner->lambda), nullptr);
    ASSERT_NE(foldedExpression(*outer->lambda), nullptr);

    EXPECT_EQ(matchOf(*inner, *outer), inner->mapped);
}

TEST(MatchTreesLambda, FoldedLambdasWithRenamedArgumentDoNotMatch)
{
    tryRegisterFunctions();

    auto inner = mapWithFoldedLambda(makeLambda("c", u64, "x", u64, binaryBody("plus")));
    auto outer = mapWithFoldedLambda(makeLambda("c", u64, "y", u64, binaryBody("plus")));

    ASSERT_NE(foldedExpression(*inner->lambda), nullptr);
    ASSERT_NE(foldedExpression(*outer->lambda), nullptr);

    EXPECT_EQ(matchOf(*inner, *outer), nullptr);
}

TEST(MatchTreesLambda, FoldedLambdaCapturingDifferentLambdasDoNotMatch)
{
    tryRegisterFunctions();

    auto function_type = std::make_shared<DataTypeFunction>(DataTypes{u64}, u64);
    auto apply_captured = [](ActionsDAG & dag, const ActionsDAG::Node & arg, const ActionsDAG::Node & captured) -> const ActionsDAG::Node &
    { return addCall(dag, "arrayMap", {&captured, &arg}, "body"); };

    /// `y -> arrayMap(f, y)` capturing a folded `f`; only the bodies of the two `f` differ.
    auto build = [&](const String & f_body)
    {
        auto result = std::make_unique<MappedDag>();
        const auto & array = result->dag.addInput("arr", std::make_shared<DataTypeArray>(u64_array));
        const auto & constant = result->dag.addColumn(constUInt64(10), u64, "c");
        const auto & f = result->dag.addFunction(makeLambda("c", u64, "x", u64, binaryBody(f_body)), {&constant}, "f");
        const auto & g = result->dag.addFunction(makeLambda("f", function_type, "y", u64_array, apply_captured), {&f}, "lambda");
        finish(*result, g, array);
        return result;
    };

    auto inner = build("plus");
    auto outer = build("multiply");

    ASSERT_NE(foldedExpression(*inner->lambda), nullptr);
    ASSERT_NE(foldedExpression(*outer->lambda), nullptr);
    ASSERT_TRUE(foldedValuesEqual(*inner, *outer));

    EXPECT_EQ(matchOf(*inner, *outer), nullptr);
}

TEST(MatchTreesLambda, FoldedLambdasCapturingDifferentSetsDoNotMatch)
{
    tryRegisterFunctions();

    auto set_type = std::make_shared<DataTypeSet>();
    auto lambda = makeLambda("s", set_type, "x", u64, binaryBody("in"));
    auto inner = mapWithFoldedLambda(lambda, constSet({1, 2}), set_type, "s");
    auto outer = mapWithFoldedLambda(lambda, constSet({3, 4}), set_type, "s");

    ASSERT_NE(foldedExpression(*inner->lambda), nullptr);
    ASSERT_NE(foldedExpression(*outer->lambda), nullptr);
    ASSERT_TRUE(foldedValuesEqual(*inner, *outer)) << "a captured set has no Field value";

    EXPECT_EQ(matchOf(*inner, *outer), nullptr);
}

TEST(MatchTreesLambda, FoldedLambdasCapturingSameSetMatch)
{
    tryRegisterFunctions();

    auto set_type = std::make_shared<DataTypeSet>();
    auto lambda = makeLambda("s", set_type, "x", u64, binaryBody("in"));
    auto inner = mapWithFoldedLambda(lambda, constSet({1, 2}), set_type, "s");
    auto outer = mapWithFoldedLambda(lambda, constSet({2, 1}), set_type, "s");

    ASSERT_NE(foldedExpression(*inner->lambda), nullptr);
    ASSERT_NE(foldedExpression(*outer->lambda), nullptr);

    EXPECT_EQ(matchOf(*inner, *outer), inner->mapped);
}

#if USE_EMBEDDED_COMPILER

namespace
{

/// `(x + c) * c`: two chained compilable functions, so JIT replaces them.
const ActionsDAG::Node & plusTimes(ActionsDAG & dag, const ActionsDAG::Node & arg, const ActionsDAG::Node & captured)
{
    const auto & sum = addCall(dag, "plus", {&arg, &captured}, "sum");
    return addCall(dag, "multiply", {&sum, &captured}, "body");
}

/// `arrayMap(lambda(c), arr)` with a column `c`: the lambda stays a `FunctionCapture` node.
std::unique_ptr<MappedDag> mapWithCapturedColumn(const FunctionOverloadResolverPtr & lambda)
{
    auto result = std::make_unique<MappedDag>();
    const auto & array = result->dag.addInput("arr", u64_array);
    const auto & captured = result->dag.addInput("c", u64);
    finish(*result, result->dag.addFunction(lambda, {&captured}, "lambda"), array);
    return result;
}

bool hasCompiledNode(const ActionsDAG & dag)
{
    return std::ranges::any_of(dag.getNodes(), [](const ActionsDAG::Node & node) { return node.is_function_compiled; });
}

ActionsDAG serializeAndDeserializeCompiling(const ActionsDAG & dag)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry serialized_sets;
    dag.serialize(out, serialized_sets);

    auto context = Context::createCopy(getContext().context);
    context->setSetting("compile_expressions", Field(true));
    context->setSetting("min_count_to_compile_expression", Field(0));

    ReadBufferFromString in(out.str());
    DeserializedSetsRegistry deserialized_sets;
    return ActionsDAG::deserialize(in, deserialized_sets, context);
}

}

TEST(MatchTreesLambda, FoldedLambdaCompiledOnOneSideMatches)
{
    tryRegisterFunctions();

    auto inner = mapWithFoldedLambda(makeLambda("c", u64, "x", u64, plusTimes, /*compile=*/false));
    auto outer = mapWithFoldedLambda(makeLambda("c", u64, "x", u64, plusTimes, /*compile=*/true));

    const auto * inner_expression = foldedExpression(*inner->lambda);
    const auto * outer_expression = foldedExpression(*outer->lambda);
    ASSERT_NE(inner_expression, nullptr);
    ASSERT_NE(outer_expression, nullptr);
    ASSERT_FALSE(hasCompiledNode(inner_expression->getAcionsDAG()));
    ASSERT_TRUE(hasCompiledNode(outer_expression->getAcionsDAG())) << "JIT must have replaced the body";
    ASSERT_NE(outer_expression->getCapture().uncompiled_actions_dag, nullptr);

    EXPECT_EQ(matchOf(*inner, *outer), inner->mapped);
    EXPECT_EQ(matchOf(*outer, *inner), outer->mapped);
}

TEST(MatchTreesLambda, CompiledLambdasWithCollidingBodiesDoNotMatch)
{
    tryRegisterFunctions();

    /// `x * x + c` and `x * c + x`: JIT reads each input once and names it by its type only.
    auto square_plus = [](ActionsDAG & dag, const ActionsDAG::Node & arg, const ActionsDAG::Node & captured) -> const ActionsDAG::Node &
    { return addCall(dag, "plus", {&addCall(dag, "multiply", {&arg, &arg}, "product"), &captured}, "body"); };
    auto times_plus = [](ActionsDAG & dag, const ActionsDAG::Node & arg, const ActionsDAG::Node & captured) -> const ActionsDAG::Node &
    { return addCall(dag, "plus", {&addCall(dag, "multiply", {&arg, &captured}, "product"), &arg}, "body"); };

    auto inner = mapWithFoldedLambda(makeLambda("c", u64, "x", u64, square_plus, /*compile=*/true));
    auto outer = mapWithFoldedLambda(makeLambda("c", u64, "x", u64, times_plus, /*compile=*/true));

    const auto * inner_expression = foldedExpression(*inner->lambda);
    const auto * outer_expression = foldedExpression(*outer->lambda);
    ASSERT_NE(inner_expression, nullptr);
    ASSERT_NE(outer_expression, nullptr);
    ASSERT_NE(inner_expression->getCapture().uncompiled_actions_dag, nullptr);
    ASSERT_NE(outer_expression->getCapture().uncompiled_actions_dag, nullptr);

    auto compiled_body = [](const FunctionExpression & expression)
    {
        const auto & dag = expression.getAcionsDAG();
        return dag.tryFindInOutputs(expression.getCapture().return_name);
    };
    const auto * inner_body = compiled_body(*inner_expression);
    const auto * outer_body = compiled_body(*outer_expression);
    ASSERT_NE(inner_body, nullptr);
    ASSERT_NE(outer_body, nullptr);
    ASSERT_TRUE(inner_body->is_function_compiled && outer_body->is_function_compiled);
    ASSERT_EQ(inner_body->function_base->getName(), outer_body->function_base->getName()) << "the compiled bodies must collide";
    ASSERT_EQ(inner_body->children.size(), outer_body->children.size());

    EXPECT_EQ(matchOf(*inner, *outer), nullptr);
}

TEST(MatchTreesLambda, DeserializedCompiledLambdaMatchesOriginal)
{
    tryRegisterFunctions();

    auto original = mapWithCapturedColumn(makeLambda("c", u64, "x", u64, plusTimes, /*compile=*/false));
    ASSERT_EQ(original->lambda->column, nullptr);

    auto deserialized = serializeAndDeserializeCompiling(original->dag);

    const FunctionCapture * capture = nullptr;
    for (const auto & node : deserialized.getNodes())
        if (const auto * function_capture = typeid_cast<const FunctionCapture *>(node.function_base.get()))
            capture = function_capture;
    ASSERT_NE(capture, nullptr);
    ASSERT_TRUE(hasCompiledNode(capture->getAcionsDAG())) << "JIT must have replaced the deserialized body";
    ASSERT_NE(capture->getCapture().uncompiled_actions_dag, nullptr);

    ASSERT_EQ(deserialized.getOutputs().size(), 1u);
    EXPECT_EQ(matchOf(original->dag.getOutputs(), deserialized, deserialized.getOutputs().front()), original->mapped);
}

TEST(MatchTreesLambda, DeserializedCompiledFoldedLambdaMatchesOriginal)
{
    tryRegisterFunctions();

    auto original = mapWithFoldedLambda(makeLambda("c", u64, "x", u64, plusTimes, /*compile=*/false));
    ASSERT_NE(foldedExpression(*original->lambda), nullptr);

    auto deserialized = serializeAndDeserializeCompiling(original->dag);

    const FunctionExpression * expression = nullptr;
    for (const auto & node : deserialized.getNodes())
        if (const auto * folded = foldedExpression(node))
            expression = folded;
    ASSERT_NE(expression, nullptr);
    ASSERT_TRUE(hasCompiledNode(expression->getAcionsDAG())) << "JIT must have replaced the deserialized body";
    ASSERT_NE(expression->getCapture().uncompiled_actions_dag, nullptr);

    ASSERT_EQ(deserialized.getOutputs().size(), 1u);
    EXPECT_EQ(matchOf(original->dag.getOutputs(), deserialized, deserialized.getOutputs().front()), original->mapped);
}

#endif
