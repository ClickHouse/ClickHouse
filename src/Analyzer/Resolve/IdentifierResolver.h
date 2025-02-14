#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Resolve/IdentifierLookup.h>

#include <Core/Joins.h>

#include <Interpreters/Context_fwd.h>

namespace DB
{

struct GetColumnsOptions;
struct IdentifierResolveScope;
struct AnalysisTableExpressionData;
class QueryExpressionsAliasVisitor ;

class QueryNode;
class JoinNode;
class ColumnNode;
class TableNode;

using ProjectionName = String;
using ProjectionNames = std::vector<ProjectionName>;

struct Settings;

class IdentifierResolver
{
public:

    explicit IdentifierResolver(std::unordered_map<QueryTreeNodePtr, ProjectionName> & node_to_projection_name_)
        : node_to_projection_name(node_to_projection_name_)
    {}

    /// Utility functions

    static bool isExpressionNodeType(QueryTreeNodeType node_type);

    static bool isFunctionExpressionNodeType(QueryTreeNodeType node_type);

    static bool isSubqueryNodeType(QueryTreeNodeType node_type);

    static bool isTableExpressionNodeType(QueryTreeNodeType node_type);

    static DataTypePtr getExpressionNodeResultTypeOrNull(const QueryTreeNodePtr & query_tree_node);

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

    static QueryTreeNodePtr convertJoinedColumnTypeToNullIfNeeded(
        const QueryTreeNodePtr & resolved_identifier,
        const JoinKind & join_kind,
        std::optional<JoinTableSide> resolved_side,
        IdentifierResolveScope & scope);

    /// Resolve identifier functions

    static IdentifierResolveResult tryResolveTableIdentifierFromDatabaseCatalog(const Identifier & table_identifier, const ContextPtr & context);

    QueryTreeNodePtr tryResolveIdentifierFromCompoundExpression(const Identifier & expression_identifier,
        size_t identifier_bind_size,
        const QueryTreeNodePtr & compound_expression,
        String compound_expression_source,
        IdentifierResolveScope & scope,
        bool can_be_not_found = false);

    IdentifierResolveResult tryResolveIdentifierFromExpressionArguments(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    static bool tryBindIdentifierToAliases(const IdentifierLookup & identifier_lookup, const IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromTableColumns(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    static bool tryBindIdentifierToTableExpression(const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        const IdentifierResolveScope & scope);

    static bool tryBindIdentifierToTableExpressions(const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        const IdentifierResolveScope & scope);

    static bool tryBindIdentifierToArrayJoinExpressions(const IdentifierLookup & identifier_lookup,
        const IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifierFromTableExpression(
        const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifierFromCrossJoin(
        const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifierFromJoin(
        const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        IdentifierResolveScope & scope);

    QueryTreeNodePtr matchArrayJoinSubcolumns(
        const QueryTreeNodePtr & array_join_column_inner_expression,
        const ColumnNode & array_join_column_expression_typed,
        const QueryTreeNodePtr & resolved_expression,
        IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveExpressionFromArrayJoinNestedExpression(
        const QueryTreeNodePtr & resolved_expression,
        IdentifierResolveScope & scope,
        ColumnNode & array_join_column_expression_typed,
        QueryTreeNodePtr & array_join_column_inner_expression);

    QueryTreeNodePtr tryResolveExpressionFromArrayJoinExpressions(
        const QueryTreeNodePtr & resolved_expression,
        const QueryTreeNodePtr & table_expression_node,
        IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifierFromArrayJoin(const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifierFromJoinTreeNode(const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & join_tree_node,
        IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifierFromJoinTree(const IdentifierLookup & identifier_lookup,
        IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifierFromStorage(
        const Identifier & identifier,
        const QueryTreeNodePtr & table_expression_node,
        const AnalysisTableExpressionData & table_expression_data,
        IdentifierResolveScope & scope,
        size_t identifier_column_qualifier_parts,
        bool can_be_not_found = false);

    /// Global expression node to projection name map
    std::unordered_map<QueryTreeNodePtr, ProjectionName> & node_to_projection_name;
};

}
