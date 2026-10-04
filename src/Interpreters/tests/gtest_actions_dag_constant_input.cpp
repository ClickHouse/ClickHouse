#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>

using namespace DB;

/// An INPUT carrying a constant lambda column stays an INPUT through constant folding,
/// so `ExpressionActions` can bind it to a required column.
TEST(ActionsDAGConstantFolding, FunctionTypedConstantInputStaysInput)
{
    tryRegisterFunctions();

    ActionsDAG lambda_dag;
    const auto & x = lambda_dag.addInput("x", std::make_shared<DataTypeUInt64>());
    lambda_dag.getOutputs().clear();
    lambda_dag.getOutputs().push_back(&x);

    auto capture = std::make_shared<FunctionCaptureOverloadResolver>(
        std::move(lambda_dag),
        ExpressionActionsSettings(getContext().context),
        Names{},
        NamesAndTypesList{{"x", std::make_shared<DataTypeUInt64>()}},
        std::make_shared<DataTypeUInt64>(),
        "x",
        /* allow_constant_folding= */ true);

    ActionsDAG producer;
    const auto & lambda = producer.addFunction(capture, {}, "lambda");
    ASSERT_TRUE(lambda.column && isColumnConst(*lambda.column));
    ASSERT_TRUE(WhichDataType(lambda.result_type).isFunction());

    ActionsDAG dag;
    const auto & input = dag.addInput(ColumnWithTypeAndName{lambda.column, lambda.result_type, "lambda"});
    dag.getOutputs().push_back(&input);

    dag.removeUnusedActions();

    ASSERT_EQ(dag.getInputs().size(), 1u);
    ASSERT_EQ(dag.getInputs().front()->type, ActionsDAG::ActionType::INPUT);

    ExpressionActions actions(std::move(dag));
    ASSERT_EQ(actions.getRequiredColumns(), Names{"lambda"});
}
