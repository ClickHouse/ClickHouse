#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>

namespace DB
{
OptimizationRulePtr createReplicatedSubplanImplementation();
}

using namespace DB;

namespace
{

SharedHeader oneColumnHeader()
{
    ColumnsWithTypeAndName columns;
    columns.emplace_back(nullptr, std::make_shared<DataTypeUInt64>(), "x");
    return std::make_shared<const Block>(Block{columns});
}

GroupExpressionPtr limitExpression()
{
    auto expression = std::make_shared<GroupExpression>(
        std::make_unique<LimitStep>(oneColumnHeader(), /*limit_=*/10, /*offset_=*/0));
    expression->inputs.push_back({.group_id = 1, .required_properties = {}});
    return expression;
}

ExpressionProperties replicatedRequirement(size_t node_count)
{
    ExpressionProperties properties;
    properties.distribution.node_count = node_count;
    properties.distribution.is_replicated = true;
    return properties;
}

}

/// The rule fails closed: a step without a proven determinism argument (a LIMIT picks
/// different rows on different nodes) never gets a replicated variant, even under a
/// replicated requirement. Widening the whitelist must consciously update this test.
TEST(CascadesReplicatedSubplan, NonWhitelistedStepRefused)
{
    auto rule = createReplicatedSubplanImplementation();
    Memo memo(getLogger("test"));

    EXPECT_FALSE(rule->checkPattern(limitExpression(), replicatedRequirement(4), memo));
}

/// The rule is goal-directed: without a replicated requirement it never fires.
TEST(CascadesReplicatedSubplan, NonReplicatedRequirementRefused)
{
    auto rule = createReplicatedSubplanImplementation();
    Memo memo(getLogger("test"));

    ExpressionProperties partitioned;
    partitioned.distribution.node_count = 4;
    EXPECT_FALSE(rule->checkPattern(limitExpression(), partitioned, memo));

    /// Replicated at a single node is not a recomputation goal either.
    EXPECT_FALSE(rule->checkPattern(limitExpression(), replicatedRequirement(1), memo));
}
