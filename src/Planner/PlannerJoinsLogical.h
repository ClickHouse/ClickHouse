#pragma once

#include <Core/Joins.h>
#include <Core/ColumnsWithTypeAndName.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/JoinNode.h>
#include <Planner/PlannerJoinTree.h>

namespace DB
{


class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

PreparedJoinStorage tryGetStorageInTableJoin(const QueryTreeNodePtr & table_expression, const PlannerContextPtr & planner_context);

JoinTreeQueryPlan buildJoinStepLogical(
    JoinTreeQueryPlan left_plan,
    JoinTreeQueryPlan right_plan,
    const NameSet & outer_scope_columns,
    const JoinNode & join_node,
    const PlannerContextPtr & planner_context);
}
