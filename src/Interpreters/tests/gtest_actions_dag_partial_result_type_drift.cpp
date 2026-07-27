#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>

using namespace DB;

namespace
{

/// `ActionsDAG::updateHeader` matches header columns to DAG input nodes BY NAME ONLY
/// (see `matchInputPositionsToHeader`), so a header column whose type differs from the type
/// the DAG's function nodes were resolved for is bound to those nodes without any type check.
/// In production that mismatch is produced by a concurrent `EXCHANGE TABLES` swapping the
/// underlying table between analysis and header computation; here it is injected directly,
/// which makes the drifted-argument path deterministic and free of any race.
ActionsDAG makeDagOverInput(const String & column_name, const DataTypePtr & resolved_type, const String & function_name)
{
    ActionsDAG dag;
    const auto & input = dag.addInput(column_name, resolved_type);

    auto function = FunctionFactory::instance().get(function_name, getContext().context);
    const auto & function_node = dag.addFunction(function, {&input}, function_name + "_result");

    dag.getOutputs().clear();
    dag.getOutputs().push_back(&function_node);
    return dag;
}

Block headerWith(const String & column_name, const DataTypePtr & type)
{
    return Block{ColumnWithTypeAndName{type->createColumn(), type, column_name}};
}

}

/// A strict function (arithmetic-like) resolved for one base type and then handed a header column
/// of another base type used to execute anyway during partial evaluation, tripping a
/// `LOGICAL_ERROR` inside the function body. In debug and sanitizer builds `LOGICAL_ERROR` is an
/// assertion failure (`handle_error_code` in `src/Common/Exception.cpp` calls
/// `abortOnFailedAssertion`), so this aborted the whole server process during query planning.
/// With the do-not-fold guard the drifted argument is simply not folded and header computation
/// completes, yielding the node's declared result type.
GTEST_TEST(ActionsDAGPartialResultTypeDrift, BaseTypeDriftDoesNotAbortHeaderComputation)
{
    tryRegisterFunctions();

    /// Resolved for `Float64` (as `03915_exchange_tables_race` does), header arrives as `Int256`.
    auto dag = makeDagOverInput("n", std::make_shared<DataTypeFloat64>(), "negate");
    const auto expected_result_type = dag.getOutputs().front()->result_type->getName();

    Block drifted_header = headerWith("n", std::make_shared<DataTypeInt256>());

    Block result;
    ASSERT_NO_THROW(result = dag.updateHeader(drifted_header));

    ASSERT_TRUE(result.has("negate_result"));
    /// The declared result type is known from analysis and must be reported unchanged: the guard
    /// skips only the fold, it must not alter the computed header.
    EXPECT_EQ(result.getByName("negate_result").type->getName(), expected_result_type);
}

/// Wrapper-only drift (`String` -> `Nullable(String)`). This is the case a wrapper-stripped type
/// comparison would miss, and it has a distinct failure mode: `materialize` returns its argument
/// type, so on drift it produced a column whose type differed from the node's resolved result
/// type and tripped the `columnMatchesType` check ("Unexpected return type from materialize"),
/// which is again a `LOGICAL_ERROR` and therefore an abort in debug/sanitizer builds.
GTEST_TEST(ActionsDAGPartialResultTypeDrift, WrapperOnlyDriftDoesNotAbortHeaderComputation)
{
    tryRegisterFunctions();

    auto dag = makeDagOverInput("s", std::make_shared<DataTypeString>(), "materialize");
    const auto expected_result_type = dag.getOutputs().front()->result_type->getName();

    Block drifted_header = headerWith("s", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));

    Block result;
    ASSERT_NO_THROW(result = dag.updateHeader(drifted_header));

    ASSERT_TRUE(result.has("materialize_result"));
    EXPECT_EQ(result.getByName("materialize_result").type->getName(), expected_result_type);
}

/// An alias between the input and the function must not hide the drift. An alias never changes a
/// value, so its declared `result_type` is a copy of the type its argument was resolved for; if the
/// partial evaluator copies a drifted column while keeping that stale declared type, the
/// argument-type check sees matching declared types and the function is executed on a mismatched
/// column anyway - the same `LOGICAL_ERROR` abort, just one node further away.
GTEST_TEST(ActionsDAGPartialResultTypeDrift, DriftBehindAliasDoesNotAbortHeaderComputation)
{
    tryRegisterFunctions();

    ActionsDAG dag;
    const auto & input = dag.addInput("n", std::make_shared<DataTypeFloat64>());
    const auto & alias = dag.addAlias(input, "n_alias");
    auto function = FunctionFactory::instance().get("negate", getContext().context);
    const auto & function_node = dag.addFunction(function, {&alias}, "negate_result");
    dag.getOutputs().clear();
    dag.getOutputs().push_back(&function_node);

    const auto expected_result_type = function_node.result_type->getName();

    Block drifted_header = headerWith("n", std::make_shared<DataTypeInt256>());

    Block result;
    ASSERT_NO_THROW(result = dag.updateHeader(drifted_header));

    ASSERT_TRUE(result.has("negate_result"));
    EXPECT_EQ(result.getByName("negate_result").type->getName(), expected_result_type);
}

/// A header column whose type did NOT drift must still be constant-folded exactly as before, so the
/// guard cannot be satisfied by disabling folding altogether. `updateHeader` is on the planning path
/// of every query, so this pins that the fix is scoped to the drifted case.
///
/// The oracle is the folded VALUE, not just the result type: a type-only assertion would also hold
/// if folding were disabled unconditionally, because the guard's fallback produces an empty column
/// of the same declared type. A constant argument makes the fold observable - it yields a
/// `ColumnConst` carrying the computed value, which the do-not-fold path never produces.
GTEST_TEST(ActionsDAGPartialResultTypeDrift, MatchingTypeStillFoldsConstant)
{
    tryRegisterFunctions();

    ActionsDAG dag;
    const auto & constant = dag.addColumn(
        ColumnConst::create(ColumnVector<Int64>::create(1, 42), 1),
        std::make_shared<DataTypeInt64>(),
        "c");
    auto function = FunctionFactory::instance().get("negate", getContext().context);
    const auto & function_node = dag.addFunction(function, {&constant}, "negate_result");
    dag.getOutputs().clear();
    dag.getOutputs().push_back(&function_node);

    Block result;
    ASSERT_NO_THROW(result = dag.updateHeader(Block{}));

    ASSERT_TRUE(result.has("negate_result"));
    const auto & folded = result.getByName("negate_result");
    ASSERT_NE(folded.column, nullptr) << "a non-drifted constant argument must still be folded";
    ASSERT_TRUE(isColumnConst(*folded.column)) << "the fold must produce a constant, not an empty column";
    EXPECT_EQ(folded.column->getInt(0), -42);
}
