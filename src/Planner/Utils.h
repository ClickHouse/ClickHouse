#pragma once

#include <Core/Block.h>
#include <Core/SortDescription.h>

#include <Parsers/IAST.h>
#include <Parsers/SelectUnionMode.h>

#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/ActionsDAG.h>

#include <Analyzer/IQueryTreeNode.h>

#include <Processors/QueryPlan/QueryPlan.h>

#include <QueryPipeline/StreamLocalLimits.h>

#include <Planner/PlannerContext.h>

#include <Storages/SelectQueryInfo.h>

namespace DB
{

/// Dump query plan
String dumpQueryPlan(QueryPlan & query_plan);

/// Dump query plan result pipeline
String dumpQueryPipeline(QueryPlan & query_plan);

/// Build common header for UNION query
Block buildCommonHeaderForUnion(const Blocks & queries_headers, SelectUnionMode union_mode);

/// Convert query node to ASTSelectQuery
ASTPtr queryNodeToSelectQuery(const QueryTreeNodePtr & query_node);

/// Build context for subquery execution
ContextPtr buildSubqueryContext(const ContextPtr & context);

/// Update mutable context for subquery execution
void updateContextForSubqueryExecution(ContextMutablePtr & mutable_context);

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

/// Returns true if query node JOIN TREE contains ARRAY JOIN node, false otherwise
bool queryHasArrayJoinInJoinTree(const QueryTreeNodePtr & query_node);

/** Returns true if query node JOIN TREE contains QUERY node with WITH TOTALS, false otherwise.
  * Function is applied recursively to subqueries in JOIN TREE.
  */
bool queryHasWithTotalsInAnySubqueryInJoinTree(const QueryTreeNodePtr & query_node);

/// Returns `and` function node that has condition nodes as its arguments
QueryTreeNodePtr mergeConditionNodes(const QueryTreeNodes & condition_nodes, const ContextPtr & context);

/// Replace tables nodes and table function nodes with dummy table nodes
using ResultReplacementMap = std::unordered_map<QueryTreeNodePtr, QueryTreeNodePtr>;
QueryTreeNodePtr replaceTablesAndTableFunctionsWithDummyTables(const QueryTreeNodePtr & query_node,
    const ContextPtr & context,
    ResultReplacementMap * result_replacement_map = nullptr);

/// Build subquery to read specified columns from table expression
QueryTreeNodePtr buildSubqueryToReadColumnsFromTableExpression(const NamesAndTypes & columns,
    const QueryTreeNodePtr & table_expression,
    const ContextPtr & context);

SelectQueryInfo buildSelectQueryInfo(const QueryTreeNodePtr & query_tree, const PlannerContextPtr & planner_context);

}
