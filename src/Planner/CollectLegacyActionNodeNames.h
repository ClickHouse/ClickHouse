#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/LegacyHeaderNames.h>
#include <Core/QueryProcessingStage.h>
#include <Planner/PlannerContext.h>

namespace DB
{

class Block;
class QueryPlan;
struct SelectQueryOptions;

/// Record the pre-26.5 spelling of every expression node name of `query_tree` in the global planner
/// context. Must run while `planner_context`, which owns the table expression data, is alive.
void collectLegacyActionNodeNames(const QueryTreeNodePtr & query_tree, const PlannerContextPtr & planner_context);

/// Legacy spelling of every entry of `header` that a pre-26.5 server spells differently. Empty for
/// `Complete` (user-visible aliases, not plan identifiers) and `QueryPlan` (the initiator ships the plan).
LegacyHeaderNames buildLegacyHeaderNames(
    const Block & header, const PlannerContextPtr & planner_context, QueryProcessingStage::Enum stage);

/// Rename the intermediate-stage output header of a secondary query to the spelling its pre-26.5
/// upstream expects. Throws `AMBIGUOUS_COLUMN_NAME` when the legacy names are not distinct.
void renameOutgoingHeaderForLegacyUpstream(
    QueryPlan & query_plan, const PlannerContextPtr & planner_context, const SelectQueryOptions & select_query_options, const ContextPtr & context);

}
