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

    /// Build a `nested(...)` FunctionNode for the given identifier prefix by combining
    /// per-field Array columns of the table expression (e.g. `loc.x`, `loc.y`).
    /// Returns nullptr if the identifier does not match any nested prefix in the table.
    static QueryTreeNodePtr tryResolveIdentifierAsNestedPrefix(
        const Identifier & identifier,
        const AnalysisTableExpressionData & table_expression_data,
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

public:
    /** Hybrid SQL-standard short-name fallback for subquery / CTE projection columns
      * (issue #87022 and friends). When `false`, `tryResolveIdentifierFromStorage` and
      * `tryResolveIdentifierFromTableExpression` skip the short-name index entirely and
      * behave exactly like master regardless of whether the
      * `analyzer_enable_short_column_names_from_subquery` setting populated the index.
      *
      * Used for two purposes:
      *   1. Two-pass join-tree resolution: pass 1 disables short-name so that any
      *      existing canonical / Nested-prefix match anywhere in the join tree wins;
      *      pass 2 re-enables it only when pass 1 found no resolution. This keeps the
      *      setting's "additive" contract — enabling it cannot change the resolution
      *      target of an identifier that already resolves on master.
      *   2. JOIN USING resolution: short-name is suppressed there because a USING column
      *      list matches by `ColumnNode::getColumnName`, which still returns the canonical
      *      dotted name. Allowing short-name in USING context would make USING accept
      *      mismatched left/right column names and then silently fail to merge them.
      */
    bool short_name_fallback_enabled = true;
};

}
