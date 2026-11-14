#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/Block.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class ColumnsDescription;
class QueryPlan;

/// plans the SELECT of a projection
/// names each expression as the query writes it, because a projection part stores its columns under these names
class ProjectionPlanner
{
public:
    /// the query has no FROM, it reads source_columns
    ProjectionPlanner(const ASTPtr & query_ast, const ColumnsDescription & source_columns, const ContextPtr & context);

    /// the functions in the actions refer to this context, so keep it while you use the actions
    ContextPtr getContext() const { return planner_context->getQueryContext(); }

    const Names & getAggregationKeys() const { return aggregation_keys; }
    const AggregateDescriptions & getAggregates() const { return aggregates; }

    /// calculates the aggregation keys and the aggregate function arguments from the source columns
    ActionsDAG buildBeforeAggregationActions();

    /// the plan that calculates a normal projection with WHERE from a block of the source columns
    QueryPlan buildPlanForNormalProjection(Block source);

    /// the plan that calculates the aggregate states of an aggregate projection from a block of the source columns
    QueryPlan buildPlanForAggregateProjection(Block source);

private:
    /// calculates the expressions from the source columns, the outputs have the stored names
    ActionsDAG buildActions(const QueryTreeNodes & expressions);

    /// the first steps of both plans: reads the block and applies WHERE
    QueryPlan buildPlanForSource(Block source);

    /// the last step of both plans: a subquery of IN is an ordinary query, so the step plans it and builds its set
    void addSubquerySetsStep(QueryPlan & plan);

    QueryTreeNodePtr query_tree;
    PlannerContextPtr planner_context;
    QueryTreeNodeToName node_to_name;
    Names aggregation_keys;
    AggregateDescriptions aggregates;
};

/// Builds the query tree of the `SELECT` of a projection over `source` and runs the analysis passes on it.
QueryTreeNodePtr buildProjectionQueryTree(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context);

}
