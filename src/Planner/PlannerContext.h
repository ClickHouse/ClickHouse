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

/** PlannerSet is wrapper around Set that is used during query planning.
  *
  * If subquery node is null, such set is already prepared for execution.
  *
  * If subquery node is not null, then set must be build from the result of the subquery.
  * If subquery node is not null, it must have QUERY or UNION type.
  */
class PlannerSet
{
public:
    /// Construct planner set that is ready for execution
    explicit PlannerSet(SetPtr set_)
        : set(std::move(set_))
    {}

    /// Construct planner set with set and subquery node
    explicit PlannerSet(SetPtr set_, QueryTreeNodePtr subquery_node_)
        : set(std::move(set_))
        , subquery_node(std::move(subquery_node_))
    {}

    /// Get set
    const SetPtr & getSet() const
    {
        return set;
    }

    /// Get subquery node
    const QueryTreeNodePtr & getSubqueryNode() const
    {
        return subquery_node;
    }

private:
    SetPtr set;

    QueryTreeNodePtr subquery_node;
};

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

    /// Get table expression node to data read only map
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

    using SetKey = std::string;

    using SetKeyToSet = std::unordered_map<String, PlannerSet>;

    /// Create set key for set source node
    static SetKey createSetKey(const QueryTreeNodePtr & set_source_node);

    /// Register set for set key
    void registerSet(const SetKey & key, PlannerSet planner_set);

    /// Returns true if set is registered for key, false otherwise
    bool hasSet(const SetKey & key) const;

    /// Get set for key, if no set is registered logical exception is thrown
    const PlannerSet & getSetOrThrow(const SetKey & key) const;

    /// Get set for key, if no set is registered null is returned
    const PlannerSet * getSetOrNull(const SetKey & key) const;

    /// Get registered sets
    const SetKeyToSet & getRegisteredSets() const
    {
        return set_key_to_set;
    }

private:
    /// Query context
    ContextPtr query_context;

    /// Global planner context
    GlobalPlannerContextPtr global_planner_context;

    /// Column node to column identifier
    std::unordered_map<QueryTreeNodePtr, ColumnIdentifier> column_node_to_column_identifier;

    /// Table expression node to data
    std::unordered_map<QueryTreeNodePtr, TableExpressionData> table_expression_node_to_data;

    /// Set key to set
    SetKeyToSet set_key_to_set;

};

using PlannerContextPtr = std::shared_ptr<PlannerContext>;

}
