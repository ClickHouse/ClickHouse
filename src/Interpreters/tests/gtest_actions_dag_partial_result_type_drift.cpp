#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActionsSettings.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}

namespace
{

/// `ActionsDAG::updateHeader` matches header columns to DAG input nodes BY NAME ONLY
/// (see `matchInputPositionsToHeader`), so a header column whose type differs from the type
/// the DAG's function nodes were resolved for is bound to those nodes without any type check.
/// Historically such a mismatch was produced by a concurrent `EXCHANGE TABLES` swapping the
/// underlying table between analysis and header computation (that root cause was fixed
/// separately by restoring the per-query storage-cache pinning). These tests do not depend on
/// any particular source of the mismatch: they inject it directly, which makes the path
/// deterministic and free of any race.
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
TEST(ActionsDAGPartialResultTypeDrift, BaseTypeDriftDoesNotAbortHeaderComputation)
{
    tryRegisterFunctions();

    /// Resolved for `Float64` (as `03915_exchange_tables_race` does), header arrives as `Int256`.
    auto dag = makeDagOverInput("n", std::make_shared<DataTypeFloat64>(), "negate");
    const auto expected_result_type = dag.getOutputs().front()->result_type->getName();

    Block drifted_header = headerWith("n", std::make_shared<DataTypeInt256>());

    Block result;
    ASSERT_NO_THROW(result = dag.updateHeader(drifted_header));

    ASSERT_TRUE(result.has("negate_result"));
    const auto & folded = result.getByName("negate_result");
    /// The declared result type is known from analysis and must be reported unchanged: the guard
    /// skips only the fold, it must not alter the computed header.
    EXPECT_EQ(folded.type->getName(), expected_result_type);
    /// Pin the shape of the zero-row fallback as well, since the type alone does not distinguish it
    /// from a null column or from a fabricated default constant. It must be a real but EMPTY and
    /// NON-CONSTANT column: a constant would present a definitive folded value to header consumers,
    /// which is exactly what skipping the fold is meant to avoid.
    ASSERT_NE(folded.column, nullptr);
    EXPECT_EQ(folded.column->size(), 0u);
    EXPECT_FALSE(isColumnConst(*folded.column));
}

/// Wrapper-only drift (`String` -> `Nullable(String)`). This is the case a wrapper-stripped type
/// comparison would miss, and it has a distinct failure mode: `materialize` returns its argument
/// type, so on drift it produced a column whose type differed from the node's resolved result
/// type and tripped the `columnMatchesType` check ("Unexpected return type from materialize"),
/// which is again a `LOGICAL_ERROR` and therefore an abort in debug/sanitizer builds.
TEST(ActionsDAGPartialResultTypeDrift, WrapperOnlyDriftDoesNotAbortHeaderComputation)
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
TEST(ActionsDAGPartialResultTypeDrift, DriftBehindAliasDoesNotAbortHeaderComputation)
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

/// `ARRAY_JOIN` must not hide the drift either. It extracts the nested column of its argument, so
/// keeping the declared nested type from analysis while extracting a drifted one would let a
/// downstream function be executed on a mismatched column, exactly as an alias would.
TEST(ActionsDAGPartialResultTypeDrift, DriftBehindArrayJoinDoesNotAbortHeaderComputation)
{
    tryRegisterFunctions();

    ActionsDAG dag;
    const auto & input = dag.addInput("a", std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>()));
    const auto & array_join = dag.addArrayJoin(input, "a_array_join");
    auto function = FunctionFactory::instance().get("negate", getContext().context);
    const auto & function_node = dag.addFunction(function, {&array_join}, "negate_result");
    dag.getOutputs().clear();
    dag.getOutputs().push_back(&function_node);

    const auto expected_result_type = function_node.result_type->getName();

    auto drifted_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt256>());
    Block drifted_header = headerWith("a", drifted_type);

    Block result;
    ASSERT_NO_THROW(result = dag.updateHeader(drifted_header));

    ASSERT_TRUE(result.has("negate_result"));
    EXPECT_EQ(result.getByName("negate_result").type->getName(), expected_result_type);
}

/// Drift must not produce a WRONG folded value either, which no type check can catch on its own:
/// `isNullable` derives its result from the argument type alone, so its result type stays `UInt8`
/// whether or not the argument drifted. Resolved for `String` it folds to 0; handed a
/// `Nullable(String)` column it would fold to 1 - a definitive but wrong value.
///
/// This is also the `input_rows_count == 1` contract. `evaluatePartialResult` with one row is used
/// by the optimizer callers (JOIN rewrites, shard skipping, virtual-column path extraction), which
/// treat any non-null column as a definitive folded value, so on drift the column must stay null and
/// route them through their "unknown value" path rather than hand them a fabricated 1.
TEST(ActionsDAGPartialResultTypeDrift, WrapperOnlyDriftFoldsNoValueForOneRowCallers)
{
    tryRegisterFunctions();

    auto dag = makeDagOverInput("s", std::make_shared<DataTypeString>(), "isNullable");

    auto drifted_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    ActionsDAG::IntermediateExecutionResult node_to_column;
    node_to_column[dag.getInputs().front()]
        = ColumnWithTypeAndName{drifted_type->createColumnConstWithDefaultValue(1), drifted_type, "s"};

    ColumnsWithTypeAndName result;
    ASSERT_NO_THROW(
        result = ActionsDAG::evaluatePartialResult(node_to_column, dag.getOutputs(), /* input_rows_count= */ 1, {}));

    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result.front().column, nullptr)
        << "a drifted argument must not fold to a definitive value for the one-row callers";
}

/// The zero-row fallback builds an empty column of the node's declared result type, but one result
/// type cannot be instantiated: `DataTypeFunction` inherits `IDataTypeDummy::createColumn`, which
/// throws `NOT_IMPLEMENTED`. (`DataTypeNothing` and `DataTypeSet` derive from the same base but do
/// override `createColumn`, so they are built normally.) A captured lambda is an ordinary `FUNCTION`
/// node whose result type is `DataTypeFunction`, so if one of its captured arguments is bound to a
/// differently-typed header column, skipping the fold must not turn into that error - the column is
/// simply left null.
TEST(ActionsDAGPartialResultTypeDrift, DriftOnNonInstantiableResultTypeIsSkippedCleanly)
{
    tryRegisterFunctions();

    /// The lambda body is `c`, i.e. it just returns its captured argument.
    ActionsDAG lambda_dag;
    const auto & captured = lambda_dag.addInput("c", std::make_shared<DataTypeFloat64>());
    lambda_dag.getOutputs().clear();
    lambda_dag.getOutputs().push_back(&captured);

    auto capture = std::make_shared<FunctionCaptureOverloadResolver>(
        std::move(lambda_dag),
        ExpressionActionsSettings(getContext().context),
        Names{"c"},
        NamesAndTypesList{},
        std::make_shared<DataTypeFloat64>(),
        "c",
        /* allow_constant_folding= */ true);

    ActionsDAG dag;
    const auto & input = dag.addInput("c", std::make_shared<DataTypeFloat64>());
    const auto & capture_node = dag.addFunction(capture, {&input}, "lambda_result");
    dag.getOutputs().clear();
    dag.getOutputs().push_back(&capture_node);

    ASSERT_TRUE(typeid_cast<const DataTypeFunction *>(capture_node.result_type.get()))
        << "this case is only meaningful while a captured lambda's result type is DataTypeFunction";

    Block drifted_header = headerWith("c", std::make_shared<DataTypeInt256>());

    Block result;
    ASSERT_NO_THROW(result = dag.updateHeader(drifted_header));
    ASSERT_TRUE(result.has("lambda_result"));
    EXPECT_EQ(result.getByName("lambda_result").column, nullptr);
}

/// A captured lambda consumed by a higher-order function is the case that matters in practice: the
/// capture is a CHILD of `arrayMap`, not a terminal output. Its column stays null (see above), so the
/// consuming function reports a recoverable `NOT_FOUND_COLUMN_IN_BLOCK`. That is the improvement being
/// pinned here: executing the stale capture instead raises
/// `Cannot capture column N because it has incompatible type`, a `LOGICAL_ERROR` that aborts the
/// server in debug and sanitizer builds.
TEST(ActionsDAGPartialResultTypeDrift, DriftInCapturedLambdaUnderArrayMapIsRecoverable)
{
    tryRegisterFunctions();

    /// Lambda `x -> x + c`, capturing `c`.
    ActionsDAG lambda_dag;
    const auto & captured = lambda_dag.addInput("c", std::make_shared<DataTypeFloat64>());
    const auto & lambda_argument = lambda_dag.addInput("x", std::make_shared<DataTypeFloat64>());
    auto plus = FunctionFactory::instance().get("plus", getContext().context);
    const auto & body = lambda_dag.addFunction(plus, {&lambda_argument, &captured}, "body");
    lambda_dag.getOutputs().clear();
    lambda_dag.getOutputs().push_back(&body);

    auto capture = std::make_shared<FunctionCaptureOverloadResolver>(
        std::move(lambda_dag),
        ExpressionActionsSettings(getContext().context),
        Names{"c"},
        NamesAndTypesList{{"x", std::make_shared<DataTypeFloat64>()}},
        std::make_shared<DataTypeFloat64>(),
        "body",
        /* allow_constant_folding= */ true);

    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());

    ActionsDAG dag;
    const auto & array_input = dag.addInput("arr", array_type);
    const auto & captured_input = dag.addInput("c", std::make_shared<DataTypeFloat64>());
    const auto & capture_node = dag.addFunction(capture, {&captured_input}, "lambda");
    auto array_map = FunctionFactory::instance().get("arrayMap", getContext().context);
    const auto & mapped = dag.addFunction(array_map, {&capture_node, &array_input}, "mapped");
    dag.getOutputs().clear();
    dag.getOutputs().push_back(&mapped);

    auto drifted_type = std::make_shared<DataTypeInt256>();
    Block drifted_header{
        ColumnWithTypeAndName{array_type->createColumn(), array_type, "arr"},
        ColumnWithTypeAndName{drifted_type->createColumn(), drifted_type, "c"}};

    try
    {
        dag.updateHeader(drifted_header);
        FAIL() << "a drifted capture cannot produce a usable header";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
        EXPECT_EQ(e.message().find("Cannot capture column"), std::string::npos)
            << "the stale capture must not be executed: " << e.message();
    }
}

/// The one-row path needs the `ARRAY_JOIN` type carried too. There the branch leaves the column null
/// (arrayJoin changes the row count, so it is skipped for non-header evaluation) but the TYPE is still
/// handed to the parent. If that were the stale declared type, a wrapper-sensitive folder such as
/// `isNullable` would see no difference and fold a definitive `0` for the optimizer callers instead of
/// remaining unknown.
TEST(ActionsDAGPartialResultTypeDrift, DriftBehindArrayJoinFoldsNoValueForOneRowCallers)
{
    tryRegisterFunctions();

    ActionsDAG dag;
    const auto & input = dag.addInput("a", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
    const auto & array_join = dag.addArrayJoin(input, "a_array_join");
    auto function = FunctionFactory::instance().get("isNullable", getContext().context);
    const auto & function_node = dag.addFunction(function, {&array_join}, "isNullable_result");
    dag.getOutputs().clear();
    dag.getOutputs().push_back(&function_node);

    auto drifted_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
    ActionsDAG::IntermediateExecutionResult node_to_column;
    node_to_column[dag.getInputs().front()]
        = ColumnWithTypeAndName{drifted_type->createColumnConstWithDefaultValue(1), drifted_type, "a"};

    ColumnsWithTypeAndName result;
    ASSERT_NO_THROW(
        result = ActionsDAG::evaluatePartialResult(
            node_to_column, dag.getOutputs(), /* input_rows_count= */ 1, {.allow_unknown_function_arguments = true}));

    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result.front().column, nullptr)
        << "a differently-typed array-join argument must not fold to a definitive value";
}

/// A header column whose type did NOT drift must still be constant-folded exactly as before, so the
/// guard cannot be satisfied by disabling folding altogether. `updateHeader` is on the planning path
/// of every query, so this pins that the fix is scoped to the drifted case.
///
/// The oracle is the folded VALUE, not just the result type: a type-only assertion would also hold
/// if folding were disabled unconditionally, because the guard's fallback produces an empty column
/// of the same declared type. A constant argument makes the fold observable - it yields a
/// `ColumnConst` carrying the computed value, which the do-not-fold path never produces.
TEST(ActionsDAGPartialResultTypeDrift, MatchingTypeStillFoldsConstant)
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
