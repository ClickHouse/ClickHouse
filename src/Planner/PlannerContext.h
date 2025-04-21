#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>

#include <Planner/TableExpressionData.h>
#include <Interpreters/SelectQueryOptions.h>

namespace DB
{

/** Global planner context contains common objects that are shared between each planner context.
  *
  * 1. Column identifiers.
  */

class QueryNode;
class TableNode;

struct FiltersForTableExpression
{
    std::optional<ActionsDAG> filter_actions;
    PrewhereInfoPtr prewhere_info;
};

using FiltersForTableExpressionMap = std::map<QueryTreeNodePtr, FiltersForTableExpression>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

using RawTableExpressionDataMap = std::unordered_map<QueryTreeNodePtr, TableExpressionData *>;

class GlobalPlannerContext
{
public:
    GlobalPlannerContext(
        const QueryNode * parallel_replicas_node_,
        const TableNode * parallel_replicas_table_,
        FiltersForTableExpressionMap filters_for_table_expressions_)
        : parallel_replicas_node(parallel_replicas_node_)
        , parallel_replicas_table(parallel_replicas_table_)
        , filters_for_table_expressions(std::move(filters_for_table_expressions_))
    {
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

    void collectTableExpressionDataForCorrelatedColumns(
      const QueryTreeNodePtr & table_expression_node,
      const PlannerContextPtr & planner_context);

    RawTableExpressionDataMap & getTableExpressionDataMap() noexcept { return shared_table_expression_data; }
    const RawTableExpressionDataMap & getTableExpressionDataMap() const noexcept { return shared_table_expression_data; }

    /// The query which will be executed with parallel replicas.
    /// In case if only the most inner subquery can be executed with parallel replicas, node is nullptr.
    const QueryNode * const parallel_replicas_node = nullptr;
    /// Table which is used with parallel replicas reading. Now, only one table is supported by the protocol.
    /// It is the left-most table of the query (in JOINs, UNIONs and subqueries).
    const TableNode * const parallel_replicas_table = nullptr;

    const FiltersForTableExpressionMap filters_for_table_expressions;

private:
    std::unordered_set<ColumnIdentifier> column_identifiers;

    /// Table expression node to data map for correlated columns sources
    RawTableExpressionDataMap shared_table_expression_data;
};

using GlobalPlannerContextPtr = std::shared_ptr<GlobalPlannerContext>;

class PlannerContext
{
public:
    /// Create planner context with query context and global planner context
    PlannerContext(ContextMutablePtr query_context_, GlobalPlannerContextPtr global_planner_context_, const SelectQueryOptions & select_query_options_);

    /// Create planner with modified query_context
    PlannerContext(ContextMutablePtr query_context_, PlannerContextPtr planner_context_);

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

    /// Returns false if any of following conditions met:
    /// 1. Query is executed on a follower node.
    /// 2. ignore_ast_optimizations is set.
    bool isASTLevelOptimizationAllowed() const { return is_ast_level_optimization_allowed; }

private:

    RawTableExpressionDataMap & getSharedTableExpressionDataMap() noexcept { return global_planner_context->getTableExpressionDataMap(); }

    const RawTableExpressionDataMap & getSharedTableExpressionDataMap() const noexcept { return global_planner_context->getTableExpressionDataMap(); }

    /// Query context
    ContextMutablePtr query_context;

    /// Global planner context
    GlobalPlannerContextPtr global_planner_context;

    bool is_ast_level_optimization_allowed;

    /// Column node to column identifier
    std::unordered_map<QueryTreeNodePtr, ColumnIdentifier> column_node_to_column_identifier;

    /// Table expression node to data
    std::unordered_map<QueryTreeNodePtr, TableExpressionData> table_expression_node_to_data;

    /// Set key to set
    PreparedSets prepared_sets;
};

}
