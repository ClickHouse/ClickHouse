#pragma once

#include <Interpreters/Context_fwd.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>

#include <Analyzer/Resolve/IdentifierLookup.h>
#include <Analyzer/Resolve/ScopeAliases.h>
#include <Analyzer/Resolve/TableExpressionData.h>
#include <Analyzer/Resolve/ExpressionsStack.h>

namespace DB
{

/** Projection names is name of query tree node that is used in projection part of query node.
  * Example: SELECT id FROM test_table;
  * `id` is projection name of column node
  *
  * Example: SELECT id AS id_alias FROM test_table;
  * `id_alias` is projection name of column node
  *
  * Calculation of projection names is done during expression nodes resolution. This is done this way
  * because after identifier node is resolved we lose information about identifier name. We could
  * potentially save this information in query tree node itself, but that would require to clone it in some cases.
  * Example: SELECT big_scalar_subquery AS a, a AS b, b AS c;
  * All 3 nodes in projection are the same big_scalar_subquery, but they have different projection names.
  * If we want to save it in query tree node, we have to clone subquery node that could lead to performance degradation.
  *
  * Possible solution is to separate query node metadata and query node content. So only node metadata could be cloned
  * if we want to change projection name. This solution does not seem to be easy for client of query tree because projection
  * name will be part of interface. If we potentially could hide projection names calculation in analyzer without introducing additional
  * changes in query tree structure that would be preferable.
  *
  * Currently each resolve method returns projection names array. Resolve method must compute projection names of node.
  * If node is resolved as list node this is case for `untuple` function or `matcher` result projection names array must contain projection names
  * for result nodes.
  * If node is not resolved as list node, projection names array contain single projection name for node.
  *
  * Rules for projection names:
  * 1. If node has alias. It is node projection name.
  * Except scenario where `untuple` function has alias. Example: SELECT untuple(expr) AS alias, alias.
  *
  * 2. For constant it is constant value string representation.
  *
  * 3. For identifier:
  * If identifier is resolved from JOIN TREE, we want to remove additional identifier qualifications.
  * Example: SELECT default.test_table.id FROM test_table.
  * Result projection name is `id`.
  *
  * Example: SELECT t1.id FROM test_table_1 AS t1, test_table_2 AS t2
  * In example both test_table_1, test_table_2 have `id` column.
  * In such case projection name is `t1.id` because if additional qualification is removed then column projection name `id` will be ambiguous.
  *
  * Example: SELECT default.test_table_1.id FROM test_table_1 AS t1, test_table_2 AS t2
  * In such case projection name is `test_table_1.id` because we remove unnecessary database qualification, but table name qualification cannot be removed
  * because otherwise column projection name `id` will be ambiguous.
  *
  * If identifier is not resolved from JOIN TREE. Identifier name is projection name.
  * Except scenario where `untuple` function resolved using identifier. Example: SELECT untuple(expr) AS alias, alias.
  * Example: SELECT sum(1, 1) AS value, value.
  * In such case both nodes have `value` projection names.
  *
  * Example: SELECT id AS value, value FROM test_table.
  * In such case both nodes have have `value` projection names.
  *
  * Special case is `untuple` function. If `untuple` function specified with alias, then result nodes will have alias.tuple_column_name projection names.
  * Example: SELECT cast(tuple(1), 'Tuple(id UInt64)') AS value, untuple(value) AS a;
  * Result projection names are `value`, `a.id`.
  *
  * If `untuple` function does not have alias then result nodes will have `tupleElement(untuple_expression_projection_name, 'tuple_column_name') projection names.
  *
  * Example: SELECT cast(tuple(1), 'Tuple(id UInt64)') AS value, untuple(value);
  * Result projection names are `value`, `tupleElement(value, 'id')`;
  *
  * 4. For function:
  * Projection name consists from function_name(parameters_projection_names)(arguments_projection_names).
  * Additionally if function is window function. Window node projection name is used with OVER clause.
  * Example: function_name (parameters_names)(argument_projection_names) OVER window_name;
  * Example: function_name (parameters_names)(argument_projection_names) OVER (PARTITION BY id ORDER BY id).
  * Example: function_name (parameters_names)(argument_projection_names) OVER (window_name ORDER BY id).
  *
  * 5. For lambda:
  * If it is standalone lambda that returns single expression, function projection name is used.
  * Example: WITH (x -> x + 1) AS lambda SELECT lambda(1).
  * Projection name is `lambda(1)`.
  *
  * If is it standalone lambda that returns list, projection names of list nodes are used.
  * Example: WITH (x -> *) AS lambda SELECT lambda(1) FROM test_table;
  * If test_table has two columns `id`, `value`. Then result projection names are `id`, `value`.
  *
  * If lambda is argument of function.
  * Then projection name consists from lambda(tuple(lambda_arguments)(lambda_body_projection_name));
  *
  * 6. For matcher:
  * Matched nodes projection names are used as matcher projection names.
  *
  * Matched nodes must be qualified if needed.
  * Example: SELECT * FROM test_table_1 AS t1, test_table_2 AS t2.
  * In example table test_table_1 and test_table_2 both have `id`, `value` columns.
  * Matched nodes after unqualified matcher resolve must be qualified to avoid ambiguous projection names.
  * Result projection names must be `t1.id`, `t1.value`, `t2.id`, `t2.value`.
  *
  * There are special cases
  * 1. For lambda inside APPLY matcher transformer:
  * Example: SELECT * APPLY x -> toString(x) FROM test_table.
  * In such case lambda argument projection name `x` will be replaced by matched node projection name.
  * If table has two columns `id` and `value`. Then result projection names are `toString(id)`, `toString(value)`;
  *
  * 2. For unqualified matcher when JOIN tree contains JOIN with USING.
  * Example: SELECT * FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 USING(id);
  * Result projection names must be `id`, `t1.value`, `t2.value`.
  *
  * 7. For subquery:
  * For subquery projection name consists of `_subquery_` prefix and implementation specific unique number suffix.
  * Example: SELECT (SELECT 1), (SELECT 1 UNION DISTINCT SELECT 1);
  * Result projection name can be `_subquery_1`, `subquery_2`;
  *
  * 8. For table:
  * Table node can be used in expression context only as right argument of IN function. In that case identifier is used
  * as table node projection name.
  * Example: SELECT id IN test_table FROM test_table;
  * Result projection name is `in(id, test_table)`.
  */
using ProjectionName = String;
using ProjectionNames = std::vector<ProjectionName>;
constexpr auto PROJECTION_NAME_PLACEHOLDER = "__projection_name_placeholder";

struct IdentifierResolveScope
{
    /// Construct identifier resolve scope using scope node, and parent scope
    IdentifierResolveScope(QueryTreeNodePtr scope_node_, IdentifierResolveScope * parent_scope_);

    QueryTreeNodePtr scope_node;

    IdentifierResolveScope * parent_scope = nullptr;

    ContextPtr context;

    /// Identifier lookup to result
    std::unordered_map<IdentifierLookup, IdentifierResolveState, IdentifierLookupHash> identifier_lookup_to_resolve_state;

    /// Argument can be expression like constant, column, function or table expression
    std::unordered_map<std::string, QueryTreeNodePtr> expression_argument_name_to_node;

    ScopeAliases aliases;

    /// Table column name to column node. Valid only during table ALIAS columns resolve.
    ColumnNameToColumnNodeMap column_name_to_column_node;

    /// CTE name to query node
    std::unordered_map<std::string, QueryTreeNodePtr> cte_name_to_query_node;

    /// Window name to window node
    std::unordered_map<std::string, QueryTreeNodePtr> window_name_to_window_node;

    /// Current scope expression in resolve process stack
    ExpressionsStack expressions_in_resolve_process_stack;

    /// Table expressions in resolve process
    std::unordered_set<const IQueryTreeNode *> table_expressions_in_resolve_process;

    /// Current scope expression
    std::unordered_set<IdentifierLookup, IdentifierLookupHash> non_cached_identifier_lookups_during_expression_resolve;

    /// Table expression node to data
    std::unordered_map<QueryTreeNodePtr, AnalysisTableExpressionData> table_expression_node_to_data;

    QueryTreeNodePtrWithHashIgnoreTypesSet nullable_group_by_keys;
    /// Here we count the number of nullable GROUP BY keys we met resolving expression.
    /// E.g. for a query `SELECT tuple(tuple(number)) FROM numbers(10) GROUP BY (number, tuple(number)) with cube`
    /// both `number` and `tuple(number)` would be in nullable_group_by_keys.
    /// But when we resolve `tuple(tuple(number))` we should figure out that `tuple(number)` is already a key,
    /// and we should not convert `number` to nullable.
    size_t found_nullable_group_by_key_in_scope = 0;

    /** It's possible that after a JOIN, a column in the projection has a type different from the column in the source table.
      * (For example, after join_use_nulls or USING column casted to supertype)
      * However, the column in the projection still refers to the table as its source.
      * This map is used to revert these columns back to their original columns in the source table.
      */
    QueryTreeNodePtrWithHashMap<QueryTreeNodePtr> join_columns_with_changed_types;

    /// Use identifier lookup to result cache
    bool use_identifier_lookup_to_result_cache = true;

    /// Apply nullability to aggregation keys
    bool group_by_use_nulls = false;
    /// Join retutns NULLs instead of default values
    bool join_use_nulls = false;

    /// JOINs count
    size_t joins_count = 0;

    /// Subquery depth
    size_t subquery_depth = 0;

    /** Scope join tree node for expression.
      * Valid only during analysis construction for single expression.
      */
    QueryTreeNodePtr expression_join_tree_node;

    /// Node hash to mask id map
    std::shared_ptr<std::map<IQueryTreeNode::Hash, size_t>> projection_mask_map;

    struct ResolvedFunctionsCache
    {
        FunctionOverloadResolverPtr resolver;
        FunctionBasePtr function_base;
    };

    std::map<IQueryTreeNode::Hash, ResolvedFunctionsCache> functions_cache;

    [[maybe_unused]] const IdentifierResolveScope * getNearestQueryScope() const;

    IdentifierResolveScope * getNearestQueryScope();

    AnalysisTableExpressionData & getTableExpressionDataOrThrow(const QueryTreeNodePtr & table_expression_node);

    const AnalysisTableExpressionData & getTableExpressionDataOrThrow(const QueryTreeNodePtr & table_expression_node) const;

    void pushExpressionNode(const QueryTreeNodePtr & node);

    void popExpressionNode();

    /// Dump identifier resolve scope
    [[maybe_unused]] void dump(WriteBuffer & buffer) const;

    [[maybe_unused]] String dump() const;
};

}
