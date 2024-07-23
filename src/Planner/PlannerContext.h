#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/Set.h>
#include <Interpreters/PreparedSets.h>

#include <Analyzer/IQueryTreeNode.h>

#include <Planner/TableExpressionData.h>

namespace DB
{

/** Global planner context contains common objects that are shared between each planner context.
  *
  * 1. Column identifiers.
  */
class GlobalPlannerContext
{
public:
    GlobalPlannerContext() = default;

    /** Create column identifier for column node.
      *
      * Result column identifier is added into context.
      */
    const ColumnIdentifier & createColumnIdentifier(const QueryTreeNodePtr & column_node);

    /** Create column identifier for column and column source.
      *
      * Result column identifier is added into context.
      */
    const ColumnIdentifier & createColumnIdentifier(const NameAndTypePair & column, const QueryTreeNodePtr & column_source_node);

    /// Check if context has column identifier
    bool hasColumnIdentifier(const ColumnIdentifier & column_identifier);

private:
    std::unordered_set<ColumnIdentifier> column_identifiers;
};

using GlobalPlannerContextPtr = std::shared_ptr<GlobalPlannerContext>;

class PlannerContext
{
public:
    /// Create planner context with query context and global planner context
    PlannerContext(ContextMutablePtr query_context_, GlobalPlannerContextPtr global_planner_context_);

    /// Get planner context query context
    ContextPtr getQueryContext() const
    {
        return query_context;
    }

    /// Get planner context mutable query context
    const ContextMutablePtr & getMutableQueryContext() const
    {
        return query_context;
    }

    /// Get planner context mutable query context
    ContextMutablePtr & getMutableQueryContext()
    {
        return query_context;
    }

    /// Get global planner context
    const GlobalPlannerContextPtr & getGlobalPlannerContext() const
    {
        return global_planner_context;
    }

    /// Get global planner context
    GlobalPlannerContextPtr & getGlobalPlannerContext()
    {
        return global_planner_context;
    }

    /// Get or create table expression data for table expression node.
    TableExpressionData & getOrCreateTableExpressionData(const QueryTreeNodePtr & table_expression_node);

    /** Get table expression data.
      * Exception is thrown if there are no table expression data for table expression node.
      */
    const TableExpressionData & getTableExpressionDataOrThrow(const QueryTreeNodePtr & table_expression_node) const;

    /** Get table expression data.
      * Exception is thrown if there are no table expression data for table expression node.
      */
    TableExpressionData & getTableExpressionDataOrThrow(const QueryTreeNodePtr & table_expression_node);

    /** Get table expression data.
      * Null is returned if there are no table expression data for table expression node.
      */
    const TableExpressionData * getTableExpressionDataOrNull(const QueryTreeNodePtr & table_expression_node) const;

    /** Get table expression data.
      * Null is returned if there are no table expression data for table expression node.
      */
    TableExpressionData * getTableExpressionDataOrNull(const QueryTreeNodePtr & table_expression_node);

    /// Get table expression node to data map
    const std::unordered_map<QueryTreeNodePtr, TableExpressionData> & getTableExpressionNodeToData() const
    {
        return table_expression_node_to_data;
    }

    /// Get table expression node to data map
    std::unordered_map<QueryTreeNodePtr, TableExpressionData> & getTableExpressionNodeToData()
    {
        return table_expression_node_to_data;
    }

    /** Get column node identifier.
      * For column node source check if table expression data is registered.
      * If table expression data is not registered exception is thrown.
      * In table expression data get column node identifier using column name.
      */
    const ColumnIdentifier & getColumnNodeIdentifierOrThrow(const QueryTreeNodePtr & column_node) const;

    /** Get column node identifier.
      * For column node source check if table expression data is registered.
      * If table expression data is not registered null is returned.
      * In table expression data get column node identifier or null using column name.
      */
    const ColumnIdentifier * getColumnNodeIdentifierOrNull(const QueryTreeNodePtr & column_node) const;

    using SetKey = std::string;

    /// Create set key for set source node
    static SetKey createSetKey(const DataTypePtr & left_operand_type, const QueryTreeNodePtr & set_source_node);

    PreparedSets & getPreparedSets() { return prepared_sets; }

private:
    /// Query context
    ContextMutablePtr query_context;

    /// Global planner context
    GlobalPlannerContextPtr global_planner_context;

    /// Column node to column identifier
    std::unordered_map<QueryTreeNodePtr, ColumnIdentifier> column_node_to_column_identifier;

    /// Table expression node to data
    std::unordered_map<QueryTreeNodePtr, TableExpressionData> table_expression_node_to_data;

    /// Set key to set
    PreparedSets prepared_sets;
};

using PlannerContextPtr = std::shared_ptr<PlannerContext>;

}
