#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/// Validate PREWHERE, WHERE, HAVING in query node
void validateFilters(const QueryTreeNodePtr & query_node);

struct AggregatesValidationParams
{
    bool group_by_use_nulls = false;
};

/** Validate aggregates in query node.
  *
  * 1. Check that there are no aggregate functions and GROUPING function in JOIN TREE, WHERE, PREWHERE, in another aggregate functions.
  * 2. Check that there are no window functions in JOIN TREE, WHERE, PREWHERE, HAVING, WINDOW, inside another aggregate function,
  * inside window function arguments, inside window function window definition.
  * 3. Check that there are no columns that are not specified in GROUP BY keys in HAVING, ORDER BY, PROJECTION.
  * 4. Check that there are no GROUPING functions that have arguments that are not specified in GROUP BY keys in HAVING, ORDER BY,
  * PROJECTION.
  * 5. Throws exception if there is GROUPING SETS or ROLLUP or CUBE or WITH TOTALS without aggregation.
  */
void validateAggregates(const QueryTreeNodePtr & query_node, AggregatesValidationParams params);

/** Assert that there are no function nodes with specified function name in node children.
  * Do not visit subqueries.
  */
void assertNoFunctionNodes(const QueryTreeNodePtr & node,
    std::string_view function_name,
    int exception_code,
    std::string_view exception_function_name,
    std::string_view exception_place_message);

/** Validate tree size. If size of tree is greater than max size throws exception.
  * Additionally for each node in tree, update node to tree size map.
  */
void validateTreeSize(const QueryTreeNodePtr & node,
    size_t max_size,
    std::unordered_map<QueryTreeNodePtr, size_t> & node_to_tree_size);

/**
  * Validate that correlated subqueries do not present in the context of distributed query.
  */
void validateCorrelatedSubqueries(const QueryTreeNodePtr & node);

/**
  * Validate that if correlated subquery appears in the FROM clause then it uses columns from outer query.
  */
void validateFromClause(const QueryTreeNodePtr & node);

/** Compare node with group by key node.
  * Such comparison does not take into account aliases, but checks types and column sources.
  */
bool compareGroupByKeys(const QueryTreeNodePtr & node, const QueryTreeNodePtr & group_by_key_node);

}
