#pragma once

#include <Interpreters/Context_fwd.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Resolve/IdentifierLookup.h>

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

class IdentifierResolver
{
public:

    IdentifierResolver(
        std::unordered_set<std::string_view> & ctes_in_resolve_process_,
        std::unordered_map<QueryTreeNodePtr, ProjectionName> & node_to_projection_name_)
        : ctes_in_resolve_process(ctes_in_resolve_process_)
        , node_to_projection_name(node_to_projection_name_)
    {}

    // IdentifierResolveResult tryResolveIdentifier(const IdentifierLookup & identifier_lookup,
    //     IdentifierResolveScope & scope,
    //     IdentifierResolveSettings identifier_resolve_settings = {});

    /// Utility functions

    static bool isExpressionNodeType(QueryTreeNodeType node_type);

    static bool isFunctionExpressionNodeType(QueryTreeNodeType node_type);

    static bool isSubqueryNodeType(QueryTreeNodeType node_type);

    static bool isTableExpressionNodeType(QueryTreeNodeType node_type);

    static DataTypePtr getExpressionNodeResultTypeOrNull(const QueryTreeNodePtr & query_tree_node);

    // static ProjectionName calculateFunctionProjectionName(const QueryTreeNodePtr & function_node,
    //     const ProjectionNames & parameters_projection_names,
    //     const ProjectionNames & arguments_projection_names);

    // static ProjectionName calculateWindowProjectionName(const QueryTreeNodePtr & window_node,
    //     const QueryTreeNodePtr & parent_window_node,
    //     const String & parent_window_name,
    //     const ProjectionNames & partition_by_projection_names,
    //     const ProjectionNames & order_by_projection_names,
    //     const ProjectionName & frame_begin_offset_projection_name,
    //     const ProjectionName & frame_end_offset_projection_name);

    // static ProjectionName calculateSortColumnProjectionName(const QueryTreeNodePtr & sort_column_node,
    //     const ProjectionName & sort_expression_projection_name,
    //     const ProjectionName & fill_from_expression_projection_name,
    //     const ProjectionName & fill_to_expression_projection_name,
    //     const ProjectionName & fill_step_expression_projection_name);

    static void collectCompoundExpressionValidIdentifiersForTypoCorrection(const Identifier & unresolved_identifier,
        const DataTypePtr & compound_expression_type,
        const Identifier & valid_identifier_prefix,
        std::unordered_set<Identifier> & valid_identifiers_result);

    static void collectTableExpressionValidIdentifiersForTypoCorrection(const Identifier & unresolved_identifier,
        const QueryTreeNodePtr & table_expression,
        const AnalysisTableExpressionData & table_expression_data,
        std::unordered_set<Identifier> & valid_identifiers_result);

    static void collectScopeValidIdentifiersForTypoCorrection(const Identifier & unresolved_identifier,
        const IdentifierResolveScope & scope,
        bool allow_expression_identifiers,
        bool allow_function_identifiers,
        bool allow_table_expression_identifiers,
        std::unordered_set<Identifier> & valid_identifiers_result);

    static void collectScopeWithParentScopesValidIdentifiersForTypoCorrection(const Identifier & unresolved_identifier,
        const IdentifierResolveScope & scope,
        bool allow_expression_identifiers,
        bool allow_function_identifiers,
        bool allow_table_expression_identifiers,
        std::unordered_set<Identifier> & valid_identifiers_result);

    static std::vector<String> collectIdentifierTypoHints(const Identifier & unresolved_identifier, const std::unordered_set<Identifier> & valid_identifiers);

    static QueryTreeNodePtr wrapExpressionNodeInTupleElement(QueryTreeNodePtr expression_node, IdentifierView nested_path, const ContextPtr & context);

    // QueryTreeNodePtr tryGetLambdaFromSQLUserDefinedFunctions(const std::string & function_name, ContextPtr context);

    // void evaluateScalarSubqueryIfNeeded(QueryTreeNodePtr & query_tree_node, IdentifierResolveScope & scope);

    // static void mergeWindowWithParentWindow(const QueryTreeNodePtr & window_node, const QueryTreeNodePtr & parent_window_node, IdentifierResolveScope & scope);

    // void replaceNodesWithPositionalArguments(QueryTreeNodePtr & node_list, const QueryTreeNodes & projection_nodes, IdentifierResolveScope & scope);

    // static void convertLimitOffsetExpression(QueryTreeNodePtr & expression_node, const String & expression_description, IdentifierResolveScope & scope);

    // static void validateTableExpressionModifiers(const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    // static void validateJoinTableExpressionWithoutAlias(const QueryTreeNodePtr & join_node, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    // static void checkDuplicateTableNamesOrAlias(const QueryTreeNodePtr & join_node, QueryTreeNodePtr & left_table_expr, QueryTreeNodePtr & right_table_expr, IdentifierResolveScope & scope);

    // static std::pair<bool, UInt64> recursivelyCollectMaxOrdinaryExpressions(QueryTreeNodePtr & node, QueryTreeNodes & into);

    // static void expandGroupByAll(QueryNode & query_tree_node_typed);

    // void expandOrderByAll(QueryNode & query_tree_node_typed, const Settings & settings);

    // static std::string
    // rewriteAggregateFunctionNameIfNeeded(const std::string & aggregate_function_name, NullsAction action, const ContextPtr & context);

    // static std::optional<JoinTableSide> getColumnSideFromJoinTree(const QueryTreeNodePtr & resolved_identifier, const JoinNode & join_node);

    static QueryTreeNodePtr convertJoinedColumnTypeToNullIfNeeded(
        const QueryTreeNodePtr & resolved_identifier,
        const JoinKind & join_kind,
        std::optional<JoinTableSide> resolved_side,
        IdentifierResolveScope & scope);

    /// Resolve identifier functions

    static QueryTreeNodePtr tryResolveTableIdentifierFromDatabaseCatalog(const Identifier & table_identifier, ContextPtr context);

    QueryTreeNodePtr tryResolveIdentifierFromCompoundExpression(const Identifier & expression_identifier,
        size_t identifier_bind_size,
        const QueryTreeNodePtr & compound_expression,
        String compound_expression_source,
        IdentifierResolveScope & scope,
        bool can_be_not_found = false);

    QueryTreeNodePtr tryResolveIdentifierFromExpressionArguments(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    static bool tryBindIdentifierToAliases(const IdentifierLookup & identifier_lookup, const IdentifierResolveScope & scope);

    // QueryTreeNodePtr tryResolveIdentifierFromAliases(const IdentifierLookup & identifier_lookup,
    //     IdentifierResolveScope & scope,
    //     IdentifierResolveSettings identifier_resolve_settings);

    QueryTreeNodePtr tryResolveIdentifierFromTableColumns(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    static bool tryBindIdentifierToTableExpression(const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        const IdentifierResolveScope & scope);

    static bool tryBindIdentifierToTableExpressions(const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        const IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromTableExpression(const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromJoin(const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        IdentifierResolveScope & scope);

    QueryTreeNodePtr matchArrayJoinSubcolumns(
        const QueryTreeNodePtr & array_join_column_inner_expression,
        const ColumnNode & array_join_column_expression_typed,
        const QueryTreeNodePtr & resolved_expression,
        IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveExpressionFromArrayJoinExpressions(const QueryTreeNodePtr & resolved_expression,
        const QueryTreeNodePtr & table_expression_node,
        IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromArrayJoin(const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromJoinTreeNode(const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & join_tree_node,
        IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromJoinTree(const IdentifierLookup & identifier_lookup,
        IdentifierResolveScope & scope);

    // IdentifierResolveResult tryResolveIdentifierInParentScopes(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromStorage(
        const Identifier & identifier,
        const QueryTreeNodePtr & table_expression_node,
        const AnalysisTableExpressionData & table_expression_data,
        IdentifierResolveScope & scope,
        size_t identifier_column_qualifier_parts,
        bool can_be_not_found = false);

    /// Resolve query tree nodes functions

    // void qualifyColumnNodesWithProjectionNames(const QueryTreeNodes & column_nodes,
    //     const QueryTreeNodePtr & table_expression_node,
    //     const IdentifierResolveScope & scope);

    // static GetColumnsOptions buildGetColumnsOptions(QueryTreeNodePtr & matcher_node, const ContextPtr & context);

    // using QueryTreeNodesWithNames = std::vector<std::pair<QueryTreeNodePtr, std::string>>;

    // QueryTreeNodesWithNames getMatchedColumnNodesWithNames(const QueryTreeNodePtr & matcher_node,
    //     const QueryTreeNodePtr & table_expression_node,
    //     const NamesAndTypes & matched_columns,
    //     const IdentifierResolveScope & scope);

    // void updateMatchedColumnsFromJoinUsing(QueryTreeNodesWithNames & result_matched_column_nodes_with_names, const QueryTreeNodePtr & source_table_expression, IdentifierResolveScope & scope);

    // QueryTreeNodesWithNames resolveQualifiedMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    // QueryTreeNodesWithNames resolveUnqualifiedMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    // ProjectionNames resolveMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    // ProjectionName resolveWindow(QueryTreeNodePtr & window_node, IdentifierResolveScope & scope);

    // ProjectionNames resolveLambda(const QueryTreeNodePtr & lambda_node,
    //     const QueryTreeNodePtr & lambda_node_to_resolve,
    //     const QueryTreeNodes & lambda_arguments,
    //     IdentifierResolveScope & scope);

    // ProjectionNames resolveFunction(QueryTreeNodePtr & function_node, IdentifierResolveScope & scope);

    // ProjectionNames resolveExpressionNode(QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression, bool ignore_alias = false);

    // ProjectionNames resolveExpressionNodeList(QueryTreeNodePtr & node_list, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression);

    // ProjectionNames resolveSortNodeList(QueryTreeNodePtr & sort_node_list, IdentifierResolveScope & scope);

    // void resolveGroupByNode(QueryNode & query_node_typed, IdentifierResolveScope & scope);

    // void resolveInterpolateColumnsNodeList(QueryTreeNodePtr & interpolate_node_list, IdentifierResolveScope & scope);

    // void resolveWindowNodeList(QueryTreeNodePtr & window_node_list, IdentifierResolveScope & scope);

    // NamesAndTypes resolveProjectionExpressionNodeList(QueryTreeNodePtr & projection_node_list, IdentifierResolveScope & scope);

    // void initializeQueryJoinTreeNode(QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope);

    // void initializeTableExpressionData(const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    // void resolveTableFunction(QueryTreeNodePtr & table_function_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor, bool nested_table_function);

    // void resolveArrayJoin(QueryTreeNodePtr & array_join_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor);

    // void resolveJoin(QueryTreeNodePtr & join_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor);

    // void resolveQueryJoinTreeNode(QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor);

    // void resolveQuery(const QueryTreeNodePtr & query_node, IdentifierResolveScope & scope);

    // void resolveUnion(const QueryTreeNodePtr & union_node, IdentifierResolveScope & scope);

    /// CTEs that are currently in resolve process
    std::unordered_set<std::string_view> & ctes_in_resolve_process;

    /// Global expression node to projection name map
    std::unordered_map<QueryTreeNodePtr, ProjectionName> & node_to_projection_name;

};

}
