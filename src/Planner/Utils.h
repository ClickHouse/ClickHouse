#pragma once

#include <Core/Block.h>
#include <Core/SortDescription.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/SelectUnionMode.h>

#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/ActionsDAG.h>

#include <Analyzer/IQueryTreeNode.h>

#include <Processors/QueryPlan/QueryPlan.h>

#include <QueryPipeline/StreamLocalLimits.h>

#include <Planner/PlannerContext.h>

#include <Storages/SelectQueryInfo.h>

#include <Interpreters/WindowDescription.h>

namespace DB
{

/// Dump query plan
String dumpQueryPlan(const QueryPlan & query_plan);

/// Dump query plan result pipeline
String dumpQueryPipeline(const QueryPlan & query_plan);

/// Build common header for UNION query
Block buildCommonHeaderForUnion(const Blocks & queries_headers, SelectUnionMode union_mode);

/// Convert query node to ASTSelectQuery
ASTPtr queryNodeToSelectQuery(const QueryTreeNodePtr & query_node);

/// Convert query node to ASTSelectQuery for distributed processing
ASTPtr queryNodeToDistributedSelectQuery(const QueryTreeNodePtr & query_node);

/// Build context for subquery execution
ContextPtr buildSubqueryContext(const ContextPtr & context);

/// Build limits for storage
StorageLimits buildStorageLimits(const Context & context, const SelectQueryOptions & options);

/** Convert query tree expression node into actions dag.
  * Inputs are not used for actions dag outputs.
  * Only root query tree expression node is used as actions dag output.
  */
ActionsDAG buildActionsDAGFromExpressionNode(const QueryTreeNodePtr & expression_node,
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

/// Replace table expressions from query JOIN TREE with dummy tables
using ResultReplacementMap = std::unordered_map<QueryTreeNodePtr, QueryTreeNodePtr>;
QueryTreeNodePtr replaceTableExpressionsWithDummyTables(
    const QueryTreeNodePtr & query_node,
    const QueryTreeNodes & table_nodes,
    const ContextPtr & context,
    ResultReplacementMap * result_replacement_map = nullptr);

SelectQueryInfo buildSelectQueryInfo(const QueryTreeNodePtr & query_tree, const PlannerContextPtr & planner_context);

/// Build filter for specific table_expression
FilterDAGInfo buildFilterInfo(ASTPtr filter_expression,
        const QueryTreeNodePtr & table_expression,
        PlannerContextPtr & planner_context,
        NameSet table_expression_required_names_without_filter = {});

FilterDAGInfo buildFilterInfo(QueryTreeNodePtr filter_query_tree,
        const QueryTreeNodePtr & table_expression,
        PlannerContextPtr & planner_context,
        NameSet table_expression_required_names_without_filter = {});

ASTPtr parseAdditionalResultFilter(const Settings & settings);

using UsefulSets = std::unordered_set<FutureSetPtr>;
void appendSetsFromActionsDAG(const ActionsDAG & dag, UsefulSets & useful_sets);

/// If the window frame is not set in sql, try to use the default frame from window function
/// if it have any one. Otherwise return empty.
/// If the window frame is set in sql, use it anyway.
std::optional<WindowFrame> extractWindowFrame(const FunctionNode & node);

}
