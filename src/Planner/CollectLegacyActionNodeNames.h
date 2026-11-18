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

/** Record, for every expression node of `query_tree`, the action node name a pre-26.5 server would
  * have produced, next to the name this server produces.
  *
  * Must run while `planner_context` is alive: the legacy spelling is generated from the table
  * expression data that context owns, and a rendered name cannot be translated afterwards.
  * Results go to the shared GlobalPlannerContext, which outlives per-UNION-branch contexts.
  */
void collectLegacyActionNodeNames(const QueryTreeNodePtr & query_tree, const PlannerContextPtr & planner_context);

/** Legacy spelling of every entry of `header` that a pre-26.5 server spells differently, from the
  * names collected into `planner_context`'s global context.
  *
  * Empty for `Complete`, whose header carries the query's user-visible aliases rather than plan
  * column identifiers, and for `QueryPlan`, where the initiator supplies the plan so both ends
  * already agree by construction.
  */
LegacyHeaderNames buildLegacyHeaderNames(
    const Block & header, const PlannerContextPtr & planner_context, QueryProcessingStage::Enum stage);

/** Rename the intermediate-stage header `query_plan` produces to the spelling the pre-26.5 upstream
  * of this secondary query expects, when it needs a different one. No-op otherwise.
  *
  * Applied to the outgoing header only, at the point the pipeline is built, so the identifiers the
  * planner itself works with keep their current spelling: consumers inside the planner are written
  * against that spelling.
  *
  * Throws `AMBIGUOUS_COLUMN_NAME` when the legacy spellings do not stay distinct across the header.
  */
void renameOutgoingHeaderForLegacyUpstream(
    QueryPlan & query_plan, const PlannerContextPtr & planner_context, const SelectQueryOptions & select_query_options, const ContextPtr & context);

}
