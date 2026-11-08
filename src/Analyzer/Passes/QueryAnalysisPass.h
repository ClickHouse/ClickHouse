#pragma once

#include <Interpreters/Context_fwd.h>
#include <Analyzer/IQueryTreePass.h>


namespace DB
{

/** This pass make initial query analysis.
  *
  * 1. All identifiers are resolved. Next passes can expect that there will be no IdentifierNode in query tree.
  * 2. All matchers are resolved. Next passes can expect that there will be no MatcherNode in query tree.
  * 3. All functions are resolved. Next passes can expect that for each FunctionNode its result type will be set, and it will be resolved
  * as aggregate or non aggregate function.
  * 4. All lambda expressions that are function arguments are resolved. Next passes can expect that LambaNode expression is resolved, and lambda has concrete arguments.
  * 5. All standalone lambda expressions are resolved. Next passes can expect that there will be no standalone LambaNode expressions in query.
  * 6. Constants are folded. Example: SELECT plus(1, 1).
  * Motivation for this, there are places in query tree that must contain constant:
  * Function parameters. Example: SELECT quantile(0.5)(x).
  * Functions in which result type depends on constant expression argument. Example: cast(x, 'type_name').
  * Expressions that are part of LIMIT BY LIMIT, LIMIT BY OFFSET, LIMIT, OFFSET. Example: SELECT * FROM test_table LIMIT expr.
  * Window function window frame OFFSET begin and OFFSET end.
  *
  * 7. All scalar subqueries are evaluated.
  * TODO: Scalar subqueries must be evaluated only if they are part of query tree where we must have constant. This is currently not done
  * because execution layer does not support scalar subqueries execution.
  *
  * 8. For query node.
  *
  * Projection columns are calculated. Later passes cannot change type, display name of projection column, and cannot add or remove
  * columns in projection section.
  * WITH and WINDOW sections are removed.
  *
  * 9. Query is validated. Parts that are validated:
  *
  * Constness of function parameters.
  * Constness of LIMIT and OFFSET.
  * Window functions frame. Constness of window functions frame begin OFFSET, end OFFSET.
  * In query only columns that are specified in GROUP BY keys after GROUP BY are used.
  * GROUPING function arguments are specified in GROUP BY keys.
  * No GROUPING function if there is no GROUP BY.
  * No aggregate functions in JOIN TREE, WHERE, PREWHERE, GROUP BY and inside another aggregate functions.
  * GROUP BY modifiers CUBE, ROLLUP, GROUPING SETS and WITH TOTALS.
  * Table expression modifiers are validated for table and table function nodes in JOIN TREE.
  * Table expression modifiers are disabled for subqueries in JOIN TREE.
  * For JOIN, ARRAY JOIN subqueries and table functions must have alias (Can be changed using joined_subquery_requires_alias setting).
  *
  * 10. Special functions handling:
  * Function `untuple` is handled properly.
  * Function `arrayJoin` is handled properly.
  * For functions `dictGet` and its variations and for function `joinGet` identifier as first argument is handled properly.
  * Replace `countDistinct` and `countIfDistinct` aggregate functions using setting count_distinct_implementation.
  * Add -OrNull suffix to aggregate functions if setting aggregate_functions_null_for_empty is true.
  * Function `exists` is converted into `in`.
  * Functions `in`, `notIn`, `globalIn`, `globalNotIn` converted into `nullIn`, `notNullIn`, `globalNullIn`, `globalNotNullIn` if setting transform_null_in is true.
  *
  * For function `grouping` arguments are resolved, but it is planner responsibility to initialize it with concrete grouping function
  * based on group by kind and group by keys positions.
  *
  * For function `in` and its variations arguments are resolved, but sets are not build.
  * If left and right arguments are constants constant folding is performed.
  * If right argument resolved as table, and table is not of type Set, it is replaced with query that read only ordinary columns from underlying
  * storage.
  * Example: SELECT id FROM test_table WHERE id IN test_table_other;
  * Result: SELECT id FROM test_table WHERE id IN (SELECT test_table_column FROM test_table_other);
  */
class QueryAnalysisPass final : public IQueryTreePass
{
public:
    /** Construct query analysis pass for query or union analysis.
      * Available columns are extracted from query node join tree.
      */
    explicit QueryAnalysisPass(bool only_analyze_ = false);

    /** Construct query analysis pass for expression or list of expressions analysis.
      * Available expression columns are extracted from table expression.
      * Table expression node must have query, union, table, table function type.
      */
    explicit QueryAnalysisPass(QueryTreeNodePtr table_expression_, bool only_analyze_ = false);

    String getName() override
    {
        return "QueryAnalysis";
    }

    String getDescription() override
    {
        return "Resolve type for each query expression. Replace identifiers, matchers with query expressions. Perform constant folding. Evaluate scalar subqueries.";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

private:
    QueryTreeNodePtr table_expression;
    const bool only_analyze;
};

}
