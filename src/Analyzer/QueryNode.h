#pragma once

#include <Common/SettingsChanges.h>

#include <Core/NamesAndTypes.h>
#include <Core/Field.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/TableExpressionModifiers.h>

#include <Interpreters/Context_fwd.h>

namespace DB
{

/** Query node represents query in query tree.
  *
  * Example: SELECT * FROM test_table WHERE id == 0;
  * Example: SELECT * FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 ON t1.id = t2.id;
  *
  * Query node consists of following sections.
  * 1. WITH section.
  * 2. PROJECTION section.
  * 3. JOIN TREE section.
  * Example: SELECT * FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 ON t1.id = t2.id;
  * test_table_1 AS t1 INNER JOIN test_table_2 AS t2 ON t1.id = t2.id - JOIN TREE section.
  * 4. PREWHERE section.
  * 5. WHERE section.
  * 6. GROUP BY section.
  * 7. HAVING section.
  * 8. WINDOW section.
  * Example: SELECT * FROM test_table WINDOW window AS (PARTITION BY id);
  * 9. ORDER BY section.
  * 10. INTERPOLATE section.
  * Example: SELECT * FROM test_table ORDER BY id WITH FILL INTERPOLATE (value AS value + 1);
  * value AS value + 1 - INTERPOLATE section.
  * 11. LIMIT BY limit section.
  * 12. LIMIT BY offset section.
  * 13. LIMIT BY section.
  * Example: SELECT * FROM test_table LIMIT 1 AS a OFFSET 5 AS b BY id, value;
  * 1 AS a - LIMIT BY limit section.
  * 5 AS b - LIMIT BY offset section.
  * id, value - LIMIT BY section.
  * 14. LIMIT section.
  * 15. OFFSET section.
  *
  * Query node contains settings changes that must be applied before query analysis or execution.
  * Example: SELECT * FROM test_table SETTINGS prefer_column_name_to_alias = 1, join_use_nulls = 1;
  *
  * Query node can be used as CTE.
  * Example: WITH cte_subquery AS (SELECT 1) SELECT * FROM cte_subquery;
  *
  * Query node can be used as scalar subquery.
  * Example: SELECT (SELECT 1) AS scalar_subquery.
  *
  * During query analysis pass query node must be resolved with projection columns.
  */
class QueryNode;
using QueryNodePtr = std::shared_ptr<QueryNode>;

class QueryNode final : public IQueryTreeNode
{
public:
    /// Construct query node with context and changed settings
    explicit QueryNode(ContextMutablePtr context_, SettingsChanges settings_changes_);

    /// Construct query node with context
    explicit QueryNode(ContextMutablePtr context_);

    /// Get context
    ContextPtr getContext() const
    {
        return context;
    }

    /// Get mutable context
    const ContextMutablePtr & getMutableContext() const
    {
        return context;
    }

    /// Get mutable context
    ContextMutablePtr & getMutableContext()
    {
        return context;
    }

    /// Returns true if query node has settings changes, false otherwise
    bool hasSettingsChanges() const
    {
        return !settings_changes.empty();
    }

    /// Get query node settings changes
    const SettingsChanges & getSettingsChanges() const
    {
        return settings_changes;
    }

    void clearSettingsChanges()
    {
        settings_changes.clear();
    }

    /// Returns true if query node is subquery, false otherwise
    bool isSubquery() const
    {
        return is_subquery;
    }

    /// Set query node is subquery value
    void setIsSubquery(bool is_subquery_value)
    {
        is_subquery = is_subquery_value;
    }

    /// Returns true if query node is CTE, false otherwise
    bool isCTE() const
    {
        return is_cte;
    }

    /// Set query node is CTE
    void setIsCTE(bool is_cte_value)
    {
        is_cte = is_cte_value;
    }

    /// Get query node CTE name
    const std::string & getCTEName() const
    {
        return cte_name;
    }

    /// Set query node CTE name
    void setCTEName(std::string cte_name_value)
    {
        cte_name = std::move(cte_name_value);
    }

    /// Returns true if query node has RECURSIVE WITH, false otherwise
    bool isRecursiveWith() const
    {
        return is_recursive_with;
    }

    /// Set query node RECURSIVE WITH value
    void setIsRecursiveWith(bool is_recursive_with_value)
    {
        is_recursive_with = is_recursive_with_value;
    }

    /// Returns true if query node has DISTINCT, false otherwise
    bool isDistinct() const
    {
        return is_distinct;
    }

    /// Set query node DISTINCT value
    void setIsDistinct(bool is_distinct_value)
    {
        is_distinct = is_distinct_value;
    }

    /// Returns true if query node has LIMIT WITH TIES, false otherwise
    bool isLimitWithTies() const
    {
        return is_limit_with_ties;
    }

    /// Set query node LIMIT WITH TIES value
    void setIsLimitWithTies(bool is_limit_with_ties_value)
    {
        is_limit_with_ties = is_limit_with_ties_value;
    }

    /// Returns true, if query node has GROUP BY WITH TOTALS, false otherwise
    bool isGroupByWithTotals() const
    {
        return is_group_by_with_totals;
    }

    /// Set query node GROUP BY WITH TOTALS value
    void setIsGroupByWithTotals(bool is_group_by_with_totals_value)
    {
        is_group_by_with_totals = is_group_by_with_totals_value;
    }

    /// Returns true, if query node has GROUP BY with ROLLUP modifier, false otherwise
    bool isGroupByWithRollup() const
    {
        return is_group_by_with_rollup;
    }

    /// Set query node GROUP BY with ROLLUP modifier value
    void setIsGroupByWithRollup(bool is_group_by_with_rollup_value)
    {
        is_group_by_with_rollup = is_group_by_with_rollup_value;
    }

    /// Returns true, if query node has GROUP BY with CUBE modifier, false otherwise
    bool isGroupByWithCube() const
    {
        return is_group_by_with_cube;
    }

    /// Set query node GROUP BY with CUBE modifier value
    void setIsGroupByWithCube(bool is_group_by_with_cube_value)
    {
        is_group_by_with_cube = is_group_by_with_cube_value;
    }

    /// Returns true, if query node has GROUP BY with GROUPING SETS modifier, false otherwise
    bool isGroupByWithGroupingSets() const
    {
        return is_group_by_with_grouping_sets;
    }

    /// Set query node GROUP BY with GROUPING SETS modifier value
    void setIsGroupByWithGroupingSets(bool is_group_by_with_grouping_sets_value)
    {
        is_group_by_with_grouping_sets = is_group_by_with_grouping_sets_value;
    }

    /// Returns true, if query node has GROUP BY ALL modifier, false otherwise
    bool isGroupByAll() const
    {
        return is_group_by_all;
    }

    /// Set query node GROUP BY ALL modifier value
    void setIsGroupByAll(bool is_group_by_all_value)
    {
        is_group_by_all = is_group_by_all_value;
    }

    /// Returns true, if query node has ORDER BY ALL modifier, false otherwise
    bool isOrderByAll() const
    {
        return is_order_by_all;
    }

    /// Set query node ORDER BY ALL modifier value
    void setIsOrderByAll(bool is_order_by_all_value)
    {
        is_order_by_all = is_order_by_all_value;
    }

    /// Returns true if query node WITH section is not empty, false otherwise
    bool hasWith() const
    {
        return !getWith().getNodes().empty();
    }

    /// Get WITH section
    const ListNode & getWith() const
    {
        return children[with_child_index]->as<const ListNode &>();
    }

    /// Get WITH section
    ListNode & getWith()
    {
        return children[with_child_index]->as<ListNode &>();
    }

    /// Get WITH section node
    const QueryTreeNodePtr & getWithNode() const
    {
        return children[with_child_index];
    }

    /// Get WITH section node
    QueryTreeNodePtr & getWithNode()
    {
        return children[with_child_index];
    }

    /// Get PROJECTION section
    const ListNode & getProjection() const
    {
        return children[projection_child_index]->as<const ListNode &>();
    }

    /// Get PROJECTION section
    ListNode & getProjection()
    {
        return children[projection_child_index]->as<ListNode &>();
    }

    /// Get PROJECTION section node
    const QueryTreeNodePtr & getProjectionNode() const
    {
        return children[projection_child_index];
    }

    /// Get PROJECTION section node
    QueryTreeNodePtr & getProjectionNode()
    {
        return children[projection_child_index];
    }

    /// Get JOIN TREE section node
    const QueryTreeNodePtr & getJoinTree() const
    {
        return children[join_tree_child_index];
    }

    /// Get JOIN TREE section node
    QueryTreeNodePtr & getJoinTree()
    {
        return children[join_tree_child_index];
    }

    /// Returns true if query node PREWHERE section is not empty, false otherwise
    bool hasPrewhere() const
    {
        return children[prewhere_child_index] != nullptr;
    }

    /// Get PREWHERE section node
    const QueryTreeNodePtr & getPrewhere() const
    {
        return children[prewhere_child_index];
    }

    /// Get PREWHERE section node
    QueryTreeNodePtr & getPrewhere()
    {
        return children[prewhere_child_index];
    }

    /// Returns true if query node WHERE section is not empty, false otherwise
    bool hasWhere() const
    {
        return children[where_child_index] != nullptr;
    }

    /// Get WHERE section node
    const QueryTreeNodePtr & getWhere() const
    {
        return children[where_child_index];
    }

    /// Get WHERE section node
    QueryTreeNodePtr & getWhere()
    {
        return children[where_child_index];
    }

    /// Returns true if query node GROUP BY section is not empty, false otherwise
    bool hasGroupBy() const
    {
        return !getGroupBy().getNodes().empty();
    }

    /// Get GROUP BY section
    const ListNode & getGroupBy() const
    {
        return children[group_by_child_index]->as<const ListNode &>();
    }

    /// Get GROUP BY section
    ListNode & getGroupBy()
    {
        return children[group_by_child_index]->as<ListNode &>();
    }

    /// Get GROUP BY section node
    const QueryTreeNodePtr & getGroupByNode() const
    {
        return children[group_by_child_index];
    }

    /// Get GROUP BY section node
    QueryTreeNodePtr & getGroupByNode()
    {
        return children[group_by_child_index];
    }

    /// Returns true if query node HAVING section is not empty, false otherwise
    bool hasHaving() const
    {
        return getHaving() != nullptr;
    }

    /// Get HAVING section node
    const QueryTreeNodePtr & getHaving() const
    {
        return children[having_child_index];
    }

    /// Get HAVING section node
    QueryTreeNodePtr & getHaving()
    {
        return children[having_child_index];
    }

    /// Returns true if query node WINDOW section is not empty, false otherwise
    bool hasWindow() const
    {
        return !getWindow().getNodes().empty();
    }

    /// Get WINDOW section
    const ListNode & getWindow() const
    {
        return children[window_child_index]->as<const ListNode &>();
    }

    /// Get WINDOW section
    ListNode & getWindow()
    {
        return children[window_child_index]->as<ListNode &>();
    }

    /// Get WINDOW section node
    const QueryTreeNodePtr & getWindowNode() const
    {
        return children[window_child_index];
    }

    /// Get WINDOW section node
    QueryTreeNodePtr & getWindowNode()
    {
        return children[window_child_index];
    }

    /// Returns true if query node QUALIFY section is not empty, false otherwise
    bool hasQualify() const
    {
        return getQualify() != nullptr;
    }

    /// Get QUALIFY section node
    const QueryTreeNodePtr & getQualify() const
    {
        return children[qualify_child_index];
    }

    /// Get QUALIFY section node
    QueryTreeNodePtr & getQualify()
    {
        return children[qualify_child_index];
    }

    /// Returns true if query node ORDER BY section is not empty, false otherwise
    bool hasOrderBy() const
    {
        return !getOrderBy().getNodes().empty();
    }

    /// Get ORDER BY section
    const ListNode & getOrderBy() const
    {
        return children[order_by_child_index]->as<const ListNode &>();
    }

    /// Get ORDER BY section
    ListNode & getOrderBy()
    {
        return children[order_by_child_index]->as<ListNode &>();
    }

    /// Get ORDER BY section node
    const QueryTreeNodePtr & getOrderByNode() const
    {
        return children[order_by_child_index];
    }

    /// Get ORDER BY section node
    QueryTreeNodePtr & getOrderByNode()
    {
        return children[order_by_child_index];
    }

    /// Returns true if query node INTERPOLATE section is not empty, false otherwise
    bool hasInterpolate() const
    {
        return getInterpolate() != nullptr;
    }

    /// Get INTERPOLATE section node
    const QueryTreeNodePtr & getInterpolate() const
    {
        return children[interpolate_child_index];
    }

    /// Get INTERPOLATE section node
    QueryTreeNodePtr & getInterpolate()
    {
        return children[interpolate_child_index];
    }

    /// Returns true if query node LIMIT BY LIMIT section is not empty, false otherwise
    bool hasLimitByLimit() const
    {
        return children[limit_by_limit_child_index] != nullptr;
    }

    /// Get LIMIT BY LIMIT section node
    const QueryTreeNodePtr & getLimitByLimit() const
    {
        return children[limit_by_limit_child_index];
    }

    /// Get LIMIT BY LIMIT section node
    QueryTreeNodePtr & getLimitByLimit()
    {
        return children[limit_by_limit_child_index];
    }

    /// Returns true if query node LIMIT BY OFFSET section is not empty, false otherwise
    bool hasLimitByOffset() const
    {
        return children[limit_by_offset_child_index] != nullptr;
    }

     /// Get LIMIT BY OFFSET section node
    const QueryTreeNodePtr & getLimitByOffset() const
    {
        return children[limit_by_offset_child_index];
    }

    /// Get LIMIT BY OFFSET section node
    QueryTreeNodePtr & getLimitByOffset()
    {
        return children[limit_by_offset_child_index];
    }

    /// Returns true if query node LIMIT BY section is not empty, false otherwise
    bool hasLimitBy() const
    {
        return !getLimitBy().getNodes().empty();
    }

    /// Get LIMIT BY section
    const ListNode & getLimitBy() const
    {
        return children[limit_by_child_index]->as<const ListNode &>();
    }

    /// Get LIMIT BY section
    ListNode & getLimitBy()
    {
        return children[limit_by_child_index]->as<ListNode &>();
    }

    /// Get LIMIT BY section node
    const QueryTreeNodePtr & getLimitByNode() const
    {
        return children[limit_by_child_index];
    }

    /// Get LIMIT BY section node
    QueryTreeNodePtr & getLimitByNode()
    {
        return children[limit_by_child_index];
    }

    /// Returns true if query node LIMIT section is not empty, false otherwise
    bool hasLimit() const
    {
        return children[limit_child_index] != nullptr;
    }

    /// Get LIMIT section node
    const QueryTreeNodePtr & getLimit() const
    {
        return children[limit_child_index];
    }

    /// Get LIMIT section node
    QueryTreeNodePtr & getLimit()
    {
        return children[limit_child_index];
    }

    /// Returns true if query node OFFSET section is not empty, false otherwise
    bool hasOffset() const
    {
        return children[offset_child_index] != nullptr;
    }

    /// Get OFFSET section node
    const QueryTreeNodePtr & getOffset() const
    {
        return children[offset_child_index];
    }

    /// Get OFFSET section node
    QueryTreeNodePtr & getOffset()
    {
        return children[offset_child_index];
    }

    /// Get query node projection columns
    const NamesAndTypes & getProjectionColumns() const
    {
        return projection_columns;
    }

    /// Resolve query node projection columns
    void resolveProjectionColumns(NamesAndTypes projection_columns_value);

    /// Remove unused projection columns
    void removeUnusedProjectionColumns(const std::unordered_set<std::string> & used_projection_columns);

    /// Remove unused projection columns
    void removeUnusedProjectionColumns(const std::unordered_set<size_t> & used_projection_columns_indexes);

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::QUERY;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState &, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    bool is_subquery = false;
    bool is_cte = false;
    bool is_recursive_with = false;
    bool is_distinct = false;
    bool is_limit_with_ties = false;
    bool is_group_by_with_totals = false;
    bool is_group_by_with_rollup = false;
    bool is_group_by_with_cube = false;
    bool is_group_by_with_grouping_sets = false;
    bool is_group_by_all = false;
    bool is_order_by_all = false;

    std::string cte_name;
    NamesAndTypes projection_columns;
    ContextMutablePtr context;
    SettingsChanges settings_changes;

    static constexpr size_t with_child_index = 0;
    static constexpr size_t projection_child_index = 1;
    static constexpr size_t join_tree_child_index = 2;
    static constexpr size_t prewhere_child_index = 3;
    static constexpr size_t where_child_index = 4;
    static constexpr size_t group_by_child_index = 5;
    static constexpr size_t having_child_index = 6;
    static constexpr size_t window_child_index = 7;
    static constexpr size_t qualify_child_index = 8;
    static constexpr size_t order_by_child_index = 9;
    static constexpr size_t interpolate_child_index = 10;
    static constexpr size_t limit_by_limit_child_index = 11;
    static constexpr size_t limit_by_offset_child_index = 12;
    static constexpr size_t limit_by_child_index = 13;
    static constexpr size_t limit_child_index = 14;
    static constexpr size_t offset_child_index = 15;
    static constexpr size_t children_size = offset_child_index + 1;
};

}
