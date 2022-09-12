#pragma once

#include <Core/Block.h>
#include <Core/SortDescription.h>

#include <Parsers/IAST.h>

#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/ActionsDAG.h>

#include <Analyzer/IQueryTreeNode.h>

#include <Processors/QueryPlan/QueryPlan.h>

#include <QueryPipeline/StreamLocalLimits.h>

#include <Planner/PlannerContext.h>

namespace DB
{

/// Dump query plan
String dumpQueryPlan(QueryPlan & query_plan);

/// Dump query plan result pipeline
String dumpQueryPipeline(QueryPlan & query_plan);

/// Build common header for UNION query
Block buildCommonHeaderForUnion(const Blocks & queries_headers);

/// Convert query node to ASTSelectQuery
ASTPtr queryNodeToSelectQuery(const QueryTreeNodePtr & query_node);

/// Build context for subquery execution
ContextPtr buildSubqueryContext(const ContextPtr & context);

/// Build limits for storage
StorageLimits buildStorageLimits(const Context & context, const SelectQueryOptions & options);

/** Convert query tree expression node into actions dag.
  * Inputs are not used for actions dag outputs.
  * Only root query tree expression node is used as actions dag output.
  */
ActionsDAGPtr buildActionsDAGFromExpressionNode(const QueryTreeNodePtr & expression_node,
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context);

/// Returns true if prefix sort description is prefix of full sort descriptor, false otherwise
bool sortDescriptionIsPrefix(const SortDescription & prefix, const SortDescription & full);

}
