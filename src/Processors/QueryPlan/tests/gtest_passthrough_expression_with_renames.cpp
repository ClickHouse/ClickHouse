#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

/// `isPassthroughExpressionWithRenames` decides whether the automatic parallel replicas optimization may
/// look through an `ExpressionStep` - skip over it when locating the node the replicas ship from, and let
/// it adopt its child's statistics key. The contract is a bijection from inputs to outputs: renaming and
/// reordering are allowed, computing, dropping and duplicating are not.
///
/// The cases below that hold are reachable from SQL and covered by
/// `05136_autopr_projection_is_not_peeled` and `05243_autopr_reordering_wrapper_is_looked_through`. The
/// duplicate-alias case is not: for two outputs to forward one input while the counts still match, the
/// step has to drop an input, and the analyzer prunes a column the step above it does not use, so every
/// duplication it emits differs in arity and is refused on the count alone. Asserting the predicate
/// directly is the only way to hold the "no two outputs share an input" half of the bijection in place -
/// without it, a step that ships `(a, a)` could be taken for a wrapper and priced as the `(a, b)` below it.

namespace
{

SharedHeader twoColumnHeader()
{
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "a"},
        ColumnWithTypeAndName{std::make_shared<DataTypeString>(), "b"},
    });
}

bool isPassthrough(ActionsDAG dag)
{
    ExpressionStep step(twoColumnHeader(), std::move(dag));
    return isPassthroughExpressionWithRenames(step);
}

/// A DAG over `(a UInt64, b String)` whose inputs are in header order.
ActionsDAG dagOverTwoColumns()
{
    return ActionsDAG(twoColumnHeader()->getColumnsWithTypeAndName());
}

}

TEST(PassthroughExpressionWithRenames, RenameOfEveryInputIsLookedThrough)
{
    auto dag = dagOverTwoColumns();
    const auto & inputs = dag.getInputs();
    ActionsDAG::NodeRawConstPtrs outputs;
    outputs.push_back(&dag.addAlias(*inputs[0], "x"));
    outputs.push_back(&dag.addAlias(*inputs[1], "y"));
    dag.getOutputs() = outputs;

    EXPECT_TRUE(isPassthrough(std::move(dag)));
}

TEST(PassthroughExpressionWithRenames, ReorderOfUnlikeTypesIsLookedThrough)
{
    auto dag = dagOverTwoColumns();
    const auto & inputs = dag.getInputs();
    ActionsDAG::NodeRawConstPtrs outputs;
    /// `SELECT b, a`: the header's types no longer line up position by position, which is exactly what a
    /// positional comparison of the two headers got wrong.
    outputs.push_back(&dag.addAlias(*inputs[1], "y"));
    outputs.push_back(&dag.addAlias(*inputs[0], "x"));
    dag.getOutputs() = outputs;

    EXPECT_TRUE(isPassthrough(std::move(dag)));
}

TEST(PassthroughExpressionWithRenames, OneInputAliasedTwiceIsNotLookedThrough)
{
    auto dag = dagOverTwoColumns();
    const auto & inputs = dag.getInputs();
    ActionsDAG::NodeRawConstPtrs outputs;
    /// `SELECT a AS x, a AS y` while `b` is dropped: two columns in and two out, so the arity says nothing.
    /// What leaves the step is `a + a`, which is not what came in, and for a wide `a` and a narrow `b` it
    /// is the larger of the two.
    outputs.push_back(&dag.addAlias(*inputs[0], "x"));
    outputs.push_back(&dag.addAlias(*inputs[0], "y"));
    dag.getOutputs() = outputs;

    EXPECT_FALSE(isPassthrough(std::move(dag)));
}

TEST(PassthroughExpressionWithRenames, DroppedInputIsNotLookedThrough)
{
    auto dag = dagOverTwoColumns();
    const auto & inputs = dag.getInputs();
    ActionsDAG::NodeRawConstPtrs outputs;
    outputs.push_back(&dag.addAlias(*inputs[0], "x"));
    dag.getOutputs() = outputs;

    EXPECT_FALSE(isPassthrough(std::move(dag)));
}

TEST(PassthroughExpressionWithRenames, ComputedOutputIsNotLookedThrough)
{
    tryRegisterFunctions();

    auto dag = dagOverTwoColumns();
    const auto & inputs = dag.getInputs();
    auto concat = FunctionFactory::instance().get("concat", getContext().context);
    const auto & doubled = dag.addFunction(concat, {inputs[1], inputs[1]}, "bb");
    ActionsDAG::NodeRawConstPtrs outputs;
    /// The arity, the positions and the type names all survive `concat`, and the bytes do not.
    outputs.push_back(&dag.addAlias(*inputs[0], "x"));
    outputs.push_back(&dag.addAlias(doubled, "y"));
    dag.getOutputs() = outputs;

    EXPECT_FALSE(isPassthrough(std::move(dag)));
}

TEST(PassthroughExpressionWithRenames, MaterializedConstantIsNotLookedThrough)
{
    auto dag = dagOverTwoColumns();
    const auto & inputs = dag.getInputs();
    auto type = std::make_shared<DataTypeUInt64>();
    const auto & column = dag.addColumn(type->createColumnConst(1, Field(UInt64(42))), type, "forty_two");
    ActionsDAG::NodeRawConstPtrs outputs;
    /// A column the step puts there itself, rather than one it was handed: an `INPUT` stays an `INPUT`
    /// even when its column happens to be constant, so a `COLUMN` output is always this case.
    outputs.push_back(&dag.addAlias(*inputs[0], "x"));
    outputs.push_back(&dag.addAlias(column, "y"));
    dag.getOutputs() = outputs;

    EXPECT_FALSE(isPassthrough(std::move(dag)));
}
