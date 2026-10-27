#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/CascadesParams.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Optimizer.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>

#include <optional>

using namespace DB;

/// An expression imported from the plan must be found as a duplicate of an equal expression added later,
/// for example by a rule that derives an alternative the group already has.
TEST(CascadesImportDedup, ImportedExpressionIsFoundAsDuplicate)
{
    auto query_context = Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setQueryParameters({{CascadesParams::CLUSTER_NODE_COUNT, "2"}});

    std::optional<ThreadStatus> thread_status;
    if (!CurrentThread::isInitialized())
        thread_status.emplace();
    auto thread_group = std::make_shared<ThreadGroup>(query_context, 0);
    ThreadGroupSwitcher thread_group_switcher(thread_group, ThreadName::UNKNOWN, /*allow_existing_group=*/true);

    auto header = std::make_shared<const Block>(Block{{ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x"}});
    QueryPlan plan;
    plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<SourceFromSingleChunk>(header))));
    plan.addStep(std::make_unique<ExpressionStep>(header, ActionsDAG(header->getColumnsWithTypeAndName())));

    QueryPlanOptimizationSettings optimization_settings(query_context);
    CascadesOptimizer optimizer(plan, optimization_settings);
    auto [root_group_id, _] = optimizer.addGroup(*plan.getRootNode());

    auto root_group = optimizer.getGroup(root_group_id);
    ASSERT_EQ(root_group->logical_expressions.size(), 1u);
    const auto & imported = root_group->logical_expressions.front();
    ASSERT_EQ(imported->inputs.size(), 1u);

    auto duplicate = std::make_shared<GroupExpression>(*imported);
    EXPECT_FALSE(root_group->addLogicalExpression(duplicate));
    EXPECT_EQ(root_group->logical_expressions.size(), 1u);
}
