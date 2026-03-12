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

class QueryNode;
class JoinNode;
class ColumnNode;
class TableNode;

using ProjectionName = String;
using ProjectionNames = std::vector<ProjectionName>;

struct Settings;

/// Checks whether access to a specific side of a SEMI/ANTI JOIN is allowed.
/// Used both during identifier resolution and qualified matcher resolution.
struct SemiAntiJoinSideChecker
{
    bool is_semi = false;
    bool is_anti = false;
    bool skip_left = false;
    bool skip_right = false;
    bool resolving_join_on = false;

    SemiAntiJoinSideChecker() = default;

    SemiAntiJoinSideChecker(JoinStrictness strictness, JoinKind kind, const ContextPtr & context, bool resolving_join_on_expression);

    bool shouldSkipSide(JoinTableSide side) const;

    /// Check access by comparing table expression nodes directly.
    /// Used when the side is not known upfront (e.g. qualified matcher resolution).
    void throwIfTableAccessDenied(
        const JoinNode & join_node,
        const IQueryTreeNode & table_expression_node,
        const IQueryTreeNode & node_for_error_message,
        const IQueryTreeNode & scope_node) const;
};

class IdentifierResolver
{
public:

    explicit IdentifierResolver(std::unordered_map<QueryTreeNodePtr, ProjectionName> & node_to_projection_name_)
        : node_to_projection_name(node_to_projection_name_)
    {}

    /// Utility functions

    static QueryTreeNodePtr wrapExpressionNodeInTupleElement(QueryTreeNodePtr expression_node, IdentifierView nested_path, const ContextPtr & context);

    static QueryTreeNodePtr convertJoinedColumnTypeToNullIfNeeded(
        const QueryTreeNodePtr & resolved_identifier,
        DataTypePtr result_type,
        const JoinKind & join_kind,
        std::optional<JoinTableSide> resolved_side,
        IdentifierResolveScope & scope);

    /// Resolve identifier functions

    static bool tryBindIdentifierToAliases(
        const IdentifierLookup & identifier_lookup,
        const IdentifierResolveScope & scope);

    static bool tryBindIdentifierToJoinUsingColumn(
        const IdentifierLookup & identifier_lookup,
        const IdentifierResolveScope & scope);

    static bool tryBindIdentifierToTableExpression(
        const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        const IdentifierResolveScope & scope);

    static bool tryBindIdentifierToTableExpressions(
        const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        const IdentifierResolveScope & scope);

    static bool tryBindIdentifierToArrayJoinExpressions(
        const IdentifierLookup & identifier_lookup,
        const IdentifierResolveScope & scope);

    static std::shared_ptr<TableNode> tryResolveTableIdentifier(
        const Identifier & table_identifier,
        const ContextPtr & context);

    static IdentifierResolveResult tryResolveTableIdentifierFromDatabaseCatalog(
        const Identifier & table_identifier,
        const ContextPtr & context);

    QueryTreeNodePtr tryResolveIdentifierFromCompoundExpression(
        const Identifier & expression_identifier,
        size_t identifier_bind_size,
        const QueryTreeNodePtr & compound_expression,
        String compound_expression_source,
        IdentifierResolveScope & scope,
        bool can_be_not_found = false);

    IdentifierResolveResult tryResolveIdentifierFromExpressionArguments(
        const IdentifierLookup & identifier_lookup,
        IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveExpressionFromArrayJoinExpressions(
        const QueryTreeNodePtr & resolved_expression,
        const QueryTreeNodePtr & table_expression_node,
        IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifierFromJoinTreeNode(
        const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & join_tree_node,
        IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifierFromJoinTree(
        const IdentifierLookup & identifier_lookup,
        IdentifierResolveScope & scope);

private:
    QueryTreeNodePtr tryResolveIdentifierFromTableColumns(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifierFromStorage(
        const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        const AnalysisTableExpressionData & table_expression_data,
        IdentifierResolveScope & scope,
        size_t identifier_column_qualifier_parts,
        bool can_be_not_found = false);

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

    IdentifierResolveResult tryResolveIdentifierFromArrayJoin(
        const IdentifierLookup & identifier_lookup,
        const QueryTreeNodePtr & table_expression_node,
        IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveExpressionFromArrayJoinNestedExpression(
        const QueryTreeNodePtr & resolved_expression,
        IdentifierResolveScope & scope,
        ColumnNode & array_join_column_expression_typed,
        QueryTreeNodePtr & array_join_column_inner_expression);

    /// Global expression node to projection name map
    std::unordered_map<QueryTreeNodePtr, ProjectionName> & node_to_projection_name;
};

}
