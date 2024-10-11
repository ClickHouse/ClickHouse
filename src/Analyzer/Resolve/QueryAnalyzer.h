#pragma once

#include <Interpreters/Context_fwd.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Resolve/IdentifierLookup.h>
#include <Analyzer/Resolve/IdentifierResolver.h>

#include <Core/Joins.h>
#include <Core/NamesAndTypes.h>

#include <Parsers/NullsAction.h>

namespace DB
{

struct GetColumnsOptions;
struct IdentifierResolveScope;
struct AnalysisTableExpressionData;
class QueryExpressionsAliasVisitor ;

class QueryNode;
class JoinNode;
class ColumnNode;

using ProjectionName = String;
using ProjectionNames = std::vector<ProjectionName>;

struct Settings;

/** Query analyzer implementation overview. Please check documentation in QueryAnalysisPass.h first.
  * And additional documentation for each method, where special cases are described in detail.
  *
  * Each node in query must be resolved. For each query tree node resolved state is specific.
  *
  * For constant node no resolve process exists, it is resolved during construction.
  *
  * For table node no resolve process exists, it is resolved during construction.
  *
  * For function node to be resolved parameters and arguments must be resolved, function node must be initialized with concrete aggregate or
  * non aggregate function and with result type.
  *
  * For lambda node there can be 2 different cases.
  * 1. Standalone: WITH (x -> x + 1) AS lambda SELECT lambda(1); Such lambdas are inlined in query tree during query analysis pass.
  * 2. Function arguments: WITH (x -> x + 1) AS lambda SELECT arrayMap(lambda, [1, 2, 3]); For such lambda resolution must
  * set concrete lambda arguments (initially they are identifier nodes) and resolve lambda expression body.
  *
  * For query node resolve process must resolve all its inner nodes.
  *
  * For matcher node resolve process must replace it with matched nodes.
  *
  * For identifier node resolve process must replace it with concrete non identifier node. This part is most complex because
  * for identifier resolution scopes and identifier lookup context play important part.
  *
  * ClickHouse SQL support lexical scoping for identifier resolution. Scope can be defined by query node or by expression node.
  * Expression nodes that can define scope are lambdas and table ALIAS columns.
  *
  * Identifier lookup context can be expression, function, table.
  *
  * Examples: WITH (x -> x + 1) as func SELECT func() FROM func; During function `func` resolution identifier lookup is performed
  * in function context.
  *
  * If there are no information of identifier context rules are following:
  * 1. Try to resolve identifier in expression context.
  * 2. Try to resolve identifier in function context, if it is allowed. Example: SELECT func(arguments); Here func identifier cannot be resolved in function context
  * because query projection does not support that.
  * 3. Try to resolve identifier in table context, if it is allowed. Example: SELECT table; Here table identifier cannot be resolved in function context
  * because query projection does not support that.
  *
  * TODO: This does not supported properly before, because matchers could not be resolved from aliases.
  *
  * Identifiers are resolved with following rules:
  * Resolution starts with current scope.
  * 1. Try to resolve identifier from expression scope arguments. Lambda expression arguments are greatest priority.
  * 2. Try to resolve identifier from aliases.
  * 3. Try to resolve identifier from join tree if scope is query, or if there are registered table columns in scope.
  * Steps 2 and 3 can be changed using prefer_column_name_to_alias setting.
  * 4. If it is table lookup, try to resolve identifier from CTE.
  * If identifier could not be resolved in current scope, resolution must be continued in parent scopes.
  * 5. Try to resolve identifier from parent scopes.
  *
  * Additional rules about aliases and scopes.
  * 1. Parent scope cannot refer alias from child scope.
  * 2. Child scope can refer to alias in parent scope.
  *
  * Example: SELECT arrayMap(x -> x + 1 AS a, [1,2,3]), a; Identifier a is unknown in parent scope.
  * Example: SELECT a FROM (SELECT 1 as a); Here we do not refer to alias a from child query scope. But we query it projection result, similar to tables.
  * Example: WITH 1 as a SELECT (SELECT a) as b; Here in child scope identifier a is resolved using alias from parent scope.
  *
  * Additional rules about identifier binding.
  * Bind for identifier to entity means that identifier first part match some node during analysis.
  * If other parts of identifier cannot be resolved in that node, exception must be thrown.
  *
  * Example:
  * CREATE TABLE test_table (id UInt64, compound_value Tuple(value UInt64)) ENGINE=TinyLog;
  * SELECT compound_value.value, 1 AS compound_value FROM test_table;
  * Identifier first part compound_value bound to entity with alias compound_value, but nested identifier part cannot be resolved from entity,
  * lookup should not be continued, and exception must be thrown because if lookup continues that way identifier can be resolved from join tree.
  *
  * TODO: This was not supported properly before analyzer because nested identifier could not be resolved from alias.
  *
  * More complex example:
  * CREATE TABLE test_table (id UInt64, value UInt64) ENGINE=TinyLog;
  * WITH cast(('Value'), 'Tuple (value UInt64') AS value SELECT (SELECT value FROM test_table);
  * Identifier first part value bound to test_table column value, but nested identifier part cannot be resolved from it,
  * lookup should not be continued, and exception must be thrown because if lookup continues identifier can be resolved from parent scope.
  *
  * TODO: Update exception messages
  * TODO: Table identifiers with optional UUID.
  * TODO: Lookup functions arrayReduce(sum, [1, 2, 3]);
  * TODO: Support function identifier resolve from parent query scope, if lambda in parent scope does not capture any columns.
  */

class QueryAnalyzer
{
public:
    explicit QueryAnalyzer(bool only_analyze_);
    ~QueryAnalyzer();

    void resolve(QueryTreeNodePtr & node, const QueryTreeNodePtr & table_expression, ContextPtr context);

    void resolveConstantExpression(QueryTreeNodePtr & node, const QueryTreeNodePtr & table_expression, ContextPtr context);

private:
    /// Utility functions

    static ProjectionName calculateFunctionProjectionName(const QueryTreeNodePtr & function_node,
        const ProjectionNames & parameters_projection_names,
        const ProjectionNames & arguments_projection_names);

    static ProjectionName calculateWindowProjectionName(const QueryTreeNodePtr & window_node,
        const QueryTreeNodePtr & parent_window_node,
        const String & parent_window_name,
        const ProjectionNames & partition_by_projection_names,
        const ProjectionNames & order_by_projection_names,
        const ProjectionName & frame_begin_offset_projection_name,
        const ProjectionName & frame_end_offset_projection_name);

    static ProjectionName calculateSortColumnProjectionName(const QueryTreeNodePtr & sort_column_node,
        const ProjectionName & sort_expression_projection_name,
        const ProjectionName & fill_from_expression_projection_name,
        const ProjectionName & fill_to_expression_projection_name,
        const ProjectionName & fill_step_expression_projection_name);

    QueryTreeNodePtr tryGetLambdaFromSQLUserDefinedFunctions(const std::string & function_name, ContextPtr context);

    void evaluateScalarSubqueryIfNeeded(QueryTreeNodePtr & query_tree_node, IdentifierResolveScope & scope);

    static void mergeWindowWithParentWindow(const QueryTreeNodePtr & window_node, const QueryTreeNodePtr & parent_window_node, IdentifierResolveScope & scope);

    void replaceNodesWithPositionalArguments(QueryTreeNodePtr & node_list, const QueryTreeNodes & projection_nodes, IdentifierResolveScope & scope);

    static void convertLimitOffsetExpression(QueryTreeNodePtr & expression_node, const String & expression_description, IdentifierResolveScope & scope);

    static void validateTableExpressionModifiers(const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    static void validateJoinTableExpressionWithoutAlias(const QueryTreeNodePtr & join_node, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    static void checkDuplicateTableNamesOrAlias(const QueryTreeNodePtr & join_node, QueryTreeNodePtr & left_table_expr, QueryTreeNodePtr & right_table_expr, IdentifierResolveScope & scope);

    static std::pair<bool, UInt64> recursivelyCollectMaxOrdinaryExpressions(QueryTreeNodePtr & node, QueryTreeNodes & into);

    static void expandGroupByAll(QueryNode & query_tree_node_typed);

    void expandOrderByAll(QueryNode & query_tree_node_typed, const Settings & settings);

    static std::string
    rewriteAggregateFunctionNameIfNeeded(const std::string & aggregate_function_name, NullsAction action, const ContextPtr & context);

    static std::optional<JoinTableSide> getColumnSideFromJoinTree(const QueryTreeNodePtr & resolved_identifier, const JoinNode & join_node);

    /// Resolve identifier functions

    QueryTreeNodePtr tryResolveIdentifierFromAliases(const IdentifierLookup & identifier_lookup,
        IdentifierResolveScope & scope,
        IdentifierResolveSettings identifier_resolve_settings);

    IdentifierResolveResult tryResolveIdentifierInParentScopes(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifier(const IdentifierLookup & identifier_lookup,
        IdentifierResolveScope & scope,
        IdentifierResolveSettings identifier_resolve_settings = {});

    /// Resolve query tree nodes functions

    void qualifyColumnNodesWithProjectionNames(const QueryTreeNodes & column_nodes,
        const QueryTreeNodePtr & table_expression_node,
        const IdentifierResolveScope & scope);

    static GetColumnsOptions buildGetColumnsOptions(QueryTreeNodePtr & matcher_node, const ContextPtr & context);

    using QueryTreeNodesWithNames = std::vector<std::pair<QueryTreeNodePtr, std::string>>;

    QueryTreeNodesWithNames getMatchedColumnNodesWithNames(const QueryTreeNodePtr & matcher_node,
        const QueryTreeNodePtr & table_expression_node,
        const NamesAndTypes & matched_columns,
        const IdentifierResolveScope & scope);

    void updateMatchedColumnsFromJoinUsing(QueryTreeNodesWithNames & result_matched_column_nodes_with_names, const QueryTreeNodePtr & source_table_expression, IdentifierResolveScope & scope);

    QueryTreeNodesWithNames resolveQualifiedMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    QueryTreeNodesWithNames resolveUnqualifiedMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    ProjectionNames resolveMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    ProjectionName resolveWindow(QueryTreeNodePtr & window_node, IdentifierResolveScope & scope);

    ProjectionNames resolveLambda(const QueryTreeNodePtr & lambda_node,
        const QueryTreeNodePtr & lambda_node_to_resolve,
        const QueryTreeNodes & lambda_arguments,
        IdentifierResolveScope & scope);

    ProjectionNames resolveFunction(QueryTreeNodePtr & function_node, IdentifierResolveScope & scope);

    ProjectionNames resolveExpressionNode(QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression, bool ignore_alias = false);

    ProjectionNames resolveExpressionNodeList(QueryTreeNodePtr & node_list, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression);

    ProjectionNames resolveSortNodeList(QueryTreeNodePtr & sort_node_list, IdentifierResolveScope & scope);

    void resolveGroupByNode(QueryNode & query_node_typed, IdentifierResolveScope & scope);

    void resolveInterpolateColumnsNodeList(QueryTreeNodePtr & interpolate_node_list, IdentifierResolveScope & scope);

    void resolveWindowNodeList(QueryTreeNodePtr & window_node_list, IdentifierResolveScope & scope);

    NamesAndTypes resolveProjectionExpressionNodeList(QueryTreeNodePtr & projection_node_list, IdentifierResolveScope & scope);

    void initializeQueryJoinTreeNode(QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope);

    void initializeTableExpressionData(const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    void resolveTableFunction(QueryTreeNodePtr & table_function_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor, bool nested_table_function);

    void resolveArrayJoin(QueryTreeNodePtr & array_join_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor);

    void resolveJoin(QueryTreeNodePtr & join_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor);

    void resolveQueryJoinTreeNode(QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor);

    void resolveQuery(const QueryTreeNodePtr & query_node, IdentifierResolveScope & scope);

    void resolveUnion(const QueryTreeNodePtr & union_node, IdentifierResolveScope & scope);

    /// Lambdas that are currently in resolve process
    std::unordered_set<IQueryTreeNode *> lambdas_in_resolve_process;

    /// CTEs that are currently in resolve process
    std::unordered_set<std::string_view> ctes_in_resolve_process;

    /// Function name to user defined lambda map
    std::unordered_map<std::string, QueryTreeNodePtr> function_name_to_user_defined_lambda;

    /// Array join expressions counter
    size_t array_join_expressions_counter = 1;

    /// Subquery counter
    size_t subquery_counter = 1;

    /// Global expression node to projection name map
    std::unordered_map<QueryTreeNodePtr, ProjectionName> node_to_projection_name;

    IdentifierResolver identifier_resolver; // (ctes_in_resolve_process, node_to_projection_name);

    /// Global resolve expression node to projection names map
    std::unordered_map<QueryTreeNodePtr, ProjectionNames> resolved_expressions;

    /// Global resolve expression node to tree size
    std::unordered_map<QueryTreeNodePtr, size_t> node_to_tree_size;

    /// Global scalar subquery to scalar value map
    std::unordered_map<QueryTreeNodePtrWithHash, Block> scalar_subquery_to_scalar_value_local;
    std::unordered_map<QueryTreeNodePtrWithHash, Block> scalar_subquery_to_scalar_value_global;

    const bool only_analyze;
};

}
