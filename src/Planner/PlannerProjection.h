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
/// names each stored expression as the query writes it, because a projection part stores its columns under these names
class ProjectionPlanner
{
public:
    /// the query has no FROM, it reads source_columns
    ProjectionPlanner(const ASTPtr & query_ast, const ColumnsDescription & source_columns, const ContextPtr & context);

    /// the functions in the actions refer to this context, so keep it while you use the actions
    [[nodiscard]] ContextPtr getContext() const { return planner_context->getQueryContext(); }

    [[nodiscard]] const AggregateDescriptions & getAggregates() const { return aggregates; }

    /// calculates the aggregation keys and the aggregate function arguments from the source columns
    /// stored_keys gets the keys under the names that the part stores, they are not outputs of the actions
    ActionsDAG buildBeforeAggregationActions(ActionsDAG::NodeRawConstPtrs & stored_keys);

    /// the plan that calculates a normal projection with WHERE from a block of the source columns
    /// the plan does not sort the rows: the writer of the part sorts them by the sorting key of the projection
    QueryPlan buildPlanForNormalProjection(Block source);

    /// the plan that calculates the aggregate states of an aggregate projection from a block of the source columns
    QueryPlan buildPlanForAggregateProjection(Block source);

private:
    /// calculates the expressions from the identifiers of the source columns, the outputs have the names that the planner gives
    ActionsDAG buildActions(const QueryTreeNodes & expressions);

    /// calculates the aggregation keys and the aggregate function arguments
    ActionsDAG buildKeysAndArgumentsActions();

    /// the first steps of both plans: reads the block under the identifiers of the columns and applies WHERE
    QueryPlan buildPlanForSource(Block source);

    /// the last step of both plans: a subquery of IN is an ordinary query, so the step plans it and builds its set
    void addSubquerySetsStep(QueryPlan & plan);

    QueryTreeNodePtr query_tree;
    PlannerContextPtr planner_context;
    Names aggregation_keys;
    /// the names that the planner gives to the keys, in the order of aggregation_keys
    Names planner_key_names;
    AggregateDescriptions aggregates;
};

/// Builds the query tree of the `SELECT` of a projection over `source` and runs the analysis passes on it.
QueryTreeNodePtr buildProjectionQueryTree(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context);

}
