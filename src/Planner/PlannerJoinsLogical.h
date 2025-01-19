#pragma once

#include <Core/Joins.h>
#include <Core/ColumnsWithTypeAndName.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinInfo.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/JoinNode.h>

namespace DB
{


class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

PreparedJoinStorage tryGetStorageInTableJoin(const QueryTreeNodePtr & table_expression, const PlannerContextPtr & planner_context);

std::unique_ptr<JoinStepLogical> buildJoinStepLogical(
    const Block & left_header,
    const Block & right_header,
    const NameSet & outer_scope_columns,
    const JoinNode & join_node,
    const PlannerContextPtr & planner_context);

}
