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

    /// Get set key for query node
    SetKey getSetKey(const QueryTreeNodePtr & set_source_node) const;

    /// Register set for set key
    void registerSet(const SetKey & key, SetPtr set);

    /// Get set for key, if no set is registered null is returned
    SetPtr getSetOrNull(const SetKey & key) const;

    /// Get set for key, if no set is registered logical exception is thrown
    SetPtr getSetOrThrow(const SetKey & key) const;

    /** Register subquery node for set
      * Subquery node for set node must have QUERY or UNION type and set must be initialized.
      */
    void registerSubqueryNodeForSet(const SetKey & key, SubqueryNodeForSet subquery_node_for_set);

    /// Get subquery nodes for sets
    const SetKeyToSubqueryNode & getSubqueryNodesForSets() const
    {
        return set_key_to_subquery_node;
    }
private:
    SetKeyToSet set_key_to_set;

    SetKeyToSubqueryNode set_key_to_subquery_node;
};

using GlobalPlannerContextPtr = std::shared_ptr<GlobalPlannerContext>;

class PlannerContext
{
public:
    PlannerContext(ContextPtr query_context_, GlobalPlannerContextPtr global_planner_context_);

    const ContextPtr & getQueryContext() const
    {
        return query_context;
    }

    const GlobalPlannerContextPtr & getGlobalPlannerContext() const
    {
        return global_planner_context;
    }

    GlobalPlannerContextPtr & getGlobalPlannerContext()
    {
        return global_planner_context;
    }

    const std::unordered_map<QueryTreeNodePtr, TableExpressionData> & getTableExpressionNodeToData() const
    {
        return table_expression_node_to_data;
    }

    std::unordered_map<QueryTreeNodePtr, TableExpressionData> & getTableExpressionNodeToData()
    {
        return table_expression_node_to_data;
    }

    ColumnIdentifier getColumnUniqueIdentifier(const QueryTreeNodePtr & column_source_node, std::string column_name = {});

    void registerColumnNode(const QueryTreeNodePtr & column_node, const ColumnIdentifier & column_identifier);

    const ColumnIdentifier & getColumnNodeIdentifierOrThrow(const QueryTreeNodePtr & column_node) const;

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

    size_t column_identifier_counter = 0;
};

using PlannerContextPtr = std::shared_ptr<PlannerContext>;

}
