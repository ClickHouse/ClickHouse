#pragma once

#include <Common/HashTable/Hash.h>

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/Set.h>

#include <Analyzer/IQueryTreeNode.h>

#include <Planner/TableExpressionData.h>

namespace DB
{

/// Subquery node for set
struct SubqueryNodeForSet
{
    QueryTreeNodePtr subquery_node;
    SetPtr set;
};

/** Global planner context contains common objects that are shared between each planner context.
  *
  * 1. Prepared sets.
  * 2. Subqueries for sets.
  */
class GlobalPlannerContext
{
public:
    GlobalPlannerContext() = default;

    using SetKey = std::string;
    using SetKeyToSet = std::unordered_map<String, SetPtr>;
    using SetKeyToSubqueryNode = std::unordered_map<String, SubqueryNodeForSet>;

    /// Create set key for query node
    static SetKey createSetKey(const QueryTreeNodePtr & set_source_node);

    /// Register set for set key
    void registerSet(const SetKey & key, SetPtr set);

    /// Get set for key, if no set is registered null is returned
    SetPtr getSetOrNull(const SetKey & key) const;

    /// Get set for key, if no set is registered logical exception is thrown
    SetPtr getSetOrThrow(const SetKey & key) const;

    /** Register subquery node for set.
      * Subquery node for set node must have QUERY or UNION type and set must be initialized.
      */
    void registerSubqueryNodeForSet(const SetKey & key, SubqueryNodeForSet subquery_node_for_set);

    /// Get subquery nodes for sets
    const SetKeyToSubqueryNode & getSubqueryNodesForSets() const
    {
        return set_key_to_subquery_node;
    }

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
    SetKeyToSet set_key_to_set;

    SetKeyToSubqueryNode set_key_to_subquery_node;

    std::unordered_set<ColumnIdentifier> column_identifiers;
};

using GlobalPlannerContextPtr = std::shared_ptr<GlobalPlannerContext>;

class PlannerContext
{
public:
    /// Create planner context with query context and global planner context
    PlannerContext(ContextPtr query_context_, GlobalPlannerContextPtr global_planner_context_);

    /// Get planner context query context
    const ContextPtr & getQueryContext() const
    {
        return query_context;
    }

    /// Get planner context query context
    ContextPtr & getQueryContext()
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

    /// Get table expression node to data map read only map
    const std::unordered_map<QueryTreeNodePtr, TableExpressionData> & getTableExpressionNodeToData() const
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

private:
    /// Query context
    ContextPtr query_context;

    /// Global planner context
    GlobalPlannerContextPtr global_planner_context;

    /// Column node to column identifier
    std::unordered_map<QueryTreeNodePtr, ColumnIdentifier> column_node_to_column_identifier;

    /// Table expression node to data
    std::unordered_map<QueryTreeNodePtr, TableExpressionData> table_expression_node_to_data;

};

using PlannerContextPtr = std::shared_ptr<PlannerContext>;

}
