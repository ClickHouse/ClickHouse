#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Field.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

/** Query node represents query in query tree.
  */
class QueryNode;
using QueryNodePtr = std::shared_ptr<QueryNode>;

class QueryNode final : public IQueryTreeNode
{
public:
    explicit QueryNode();

    bool isSubquery() const
    {
        return is_subquery;
    }

    void setIsSubquery(bool is_subquery_value)
    {
        is_subquery = is_subquery_value;
    }

    bool isCTE() const
    {
        return is_cte;
    }

    void setIsCTE(bool is_cte_value)
    {
        is_cte = is_cte_value;
    }

    const std::string & getCTEName() const
    {
        return cte_name;
    }

    void setCTEName(std::string cte_name_value)
    {
        cte_name = std::move(cte_name_value);
    }

    void setIsDistinct(bool is_distinct_value)
    {
        is_distinct = is_distinct_value;
    }

    bool isDistinct() const
    {
        return is_distinct;
    }

    void setIsLimitWithTies(bool is_limit_with_ties_value)
    {
        is_limit_with_ties = is_limit_with_ties_value;
    }

    bool isLimitWithTies() const
    {
        return is_limit_with_ties;
    }

    void setIsGroupByWithTotals(bool is_group_by_with_totals_value)
    {
        is_group_by_with_totals = is_group_by_with_totals_value;
    }

    bool isGroupByWithTotals() const
    {
        return is_group_by_with_totals;
    }

    void setIsGroupByWithRollup(bool is_group_by_with_rollup_value)
    {
        is_group_by_with_rollup = is_group_by_with_rollup_value;
    }

    bool isGroupByWithRollup() const
    {
        return is_group_by_with_rollup;
    }

    void setIsGroupByWithCube(bool is_group_by_with_cube_value)
    {
        is_group_by_with_cube = is_group_by_with_cube_value;
    }

    bool isGroupByWithCube() const
    {
        return is_group_by_with_cube;
    }

    void setIsGroupByWithGroupingSets(bool is_group_by_with_grouping_sets_value)
    {
        is_group_by_with_grouping_sets = is_group_by_with_grouping_sets_value;
    }

    bool isGroupByWithGroupingSets() const
    {
        return is_group_by_with_grouping_sets;
    }

    /// Return true if query node has table expression modifiers, false otherwise
    bool hasTableExpressionModifiers() const
    {
        return table_expression_modifiers.has_value();
    }

    /// Get table expression modifiers
    std::optional<TableExpressionModifiers> getTableExpressionModifiers() const
    {
        return table_expression_modifiers;
    }

    /// Set table expression modifiers
    void setTableExpressionModifiers(TableExpressionModifiers table_expression_modifiers_value)
    {
        table_expression_modifiers = std::move(table_expression_modifiers_value);
    }

    bool hasWith() const
    {
        return !getWith().getNodes().empty();
    }

    const ListNode & getWith() const
    {
        return children[with_child_index]->as<const ListNode &>();
    }

    ListNode & getWith()
    {
        return children[with_child_index]->as<ListNode &>();
    }

    const QueryTreeNodePtr & getWithNode() const
    {
        return children[with_child_index];
    }

    QueryTreeNodePtr & getWithNode()
    {
        return children[with_child_index];
    }

    const ListNode & getProjection() const
    {
        return children[projection_child_index]->as<const ListNode &>();
    }

    ListNode & getProjection()
    {
        return children[projection_child_index]->as<ListNode &>();
    }

    const QueryTreeNodePtr & getProjectionNode() const
    {
        return children[projection_child_index];
    }

    QueryTreeNodePtr & getProjectionNode()
    {
        return children[projection_child_index];
    }

    const QueryTreeNodePtr & getJoinTree() const
    {
        return children[join_tree_child_index];
    }

    QueryTreeNodePtr & getJoinTree()
    {
        return children[join_tree_child_index];
    }

    bool hasWindow() const
    {
        return !getWindow().getNodes().empty();
    }

    const ListNode & getWindow() const
    {
        return children[window_child_index]->as<const ListNode &>();
    }

    ListNode & getWindow()
    {
        return children[window_child_index]->as<ListNode &>();
    }

    const QueryTreeNodePtr & getWindowNode() const
    {
        return children[window_child_index];
    }

    QueryTreeNodePtr & getWindowNode()
    {
        return children[window_child_index];
    }

    bool hasPrewhere() const
    {
        return children[prewhere_child_index] != nullptr;
    }

    const QueryTreeNodePtr & getPrewhere() const
    {
        return children[prewhere_child_index];
    }

    QueryTreeNodePtr & getPrewhere()
    {
        return children[prewhere_child_index];
    }

    bool hasWhere() const
    {
        return children[where_child_index] != nullptr;
    }

    const QueryTreeNodePtr & getWhere() const
    {
        return children[where_child_index];
    }

    QueryTreeNodePtr & getWhere()
    {
        return children[where_child_index];
    }

    bool hasGroupBy() const
    {
        return !getGroupBy().getNodes().empty();
    }

    const ListNode & getGroupBy() const
    {
        return children[group_by_child_index]->as<ListNode &>();
    }

    ListNode & getGroupBy()
    {
        return children[group_by_child_index]->as<ListNode &>();
    }

    const QueryTreeNodePtr & getGroupByNode() const
    {
        return children[group_by_child_index];
    }

    QueryTreeNodePtr & getGroupByNode()
    {
        return children[group_by_child_index];
    }

    bool hasHaving() const
    {
        return getHaving() != nullptr;
    }

    const QueryTreeNodePtr & getHaving() const
    {
        return children[having_child_index];
    }

    QueryTreeNodePtr & getHaving()
    {
        return children[having_child_index];
    }

    bool hasOrderBy() const
    {
        return !getOrderBy().getNodes().empty();
    }

    const ListNode & getOrderBy() const
    {
        return children[order_by_child_index]->as<const ListNode &>();
    }

    ListNode & getOrderBy()
    {
        return children[order_by_child_index]->as<ListNode &>();
    }

    const QueryTreeNodePtr & getOrderByNode() const
    {
        return children[order_by_child_index];
    }

    QueryTreeNodePtr & getOrderByNode()
    {
        return children[order_by_child_index];
    }

    bool hasInterpolate() const
    {
        return getInterpolate() != nullptr;
    }

    const QueryTreeNodePtr & getInterpolate() const
    {
        return children[interpolate_child_index];
    }

    QueryTreeNodePtr & getInterpolate()
    {
        return children[interpolate_child_index];
    }

    bool hasLimitByLimit() const
    {
        return children[limit_by_limit_child_index] != nullptr;
    }

    const QueryTreeNodePtr & getLimitByLimit() const
    {
        return children[limit_by_limit_child_index];
    }

    QueryTreeNodePtr & getLimitByLimit()
    {
        return children[limit_by_limit_child_index];
    }

    bool hasLimitByOffset() const
    {
        return children[limit_by_offset_child_index] != nullptr;
    }

    const QueryTreeNodePtr & getLimitByOffset() const
    {
        return children[limit_by_offset_child_index];
    }

    QueryTreeNodePtr & getLimitByOffset()
    {
        return children[limit_by_offset_child_index];
    }

    bool hasLimitBy() const
    {
        return !getLimitBy().getNodes().empty();
    }

    const ListNode & getLimitBy() const
    {
        return children[limit_by_child_index]->as<const ListNode &>();
    }

    ListNode & getLimitBy()
    {
        return children[limit_by_child_index]->as<ListNode &>();
    }

    const QueryTreeNodePtr & getLimitByNode() const
    {
        return children[limit_by_child_index];
    }

    QueryTreeNodePtr & getLimitByNode()
    {
        return children[limit_by_child_index];
    }

    bool hasLimit() const
    {
        return children[limit_child_index] != nullptr;
    }

    const QueryTreeNodePtr & getLimit() const
    {
        return children[limit_child_index];
    }

    QueryTreeNodePtr & getLimit()
    {
        return children[limit_child_index];
    }

    bool hasOffset() const
    {
        return children[offset_child_index] != nullptr;
    }

    const QueryTreeNodePtr & getOffset() const
    {
        return children[offset_child_index];
    }

    QueryTreeNodePtr & getOffset()
    {
        return children[offset_child_index];
    }

    const NamesAndTypes & getProjectionColumns() const
    {
        return projection_columns;
    }

    void resolveProjectionColumns(NamesAndTypes projection_columns_value)
    {
        projection_columns = std::move(projection_columns_value);
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::QUERY;
    }

    String getName() const override;

    DataTypePtr getResultType() const override
    {
        if (constant_value)
            return constant_value->getType();

        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Method getResultType is not supported for non scalar query node");
    }

    /// Perform constant folding for scalar subquery node
    void performConstantFolding(ConstantValuePtr constant_folded_value)
    {
        constant_value = std::move(constant_folded_value);
    }

    ConstantValuePtr getConstantValueOrNull() const override
    {
        return constant_value;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState &) const override;

protected:
    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

private:
    bool is_subquery = false;
    bool is_cte = false;
    bool is_distinct = false;
    bool is_limit_with_ties = false;
    bool is_group_by_with_totals = false;
    bool is_group_by_with_rollup = false;
    bool is_group_by_with_cube = false;
    bool is_group_by_with_grouping_sets = false;

    std::string cte_name;
    NamesAndTypes projection_columns;
    ConstantValuePtr constant_value;
    std::optional<TableExpressionModifiers> table_expression_modifiers;

    static constexpr size_t with_child_index = 0;
    static constexpr size_t projection_child_index = 1;
    static constexpr size_t join_tree_child_index = 2;
    static constexpr size_t prewhere_child_index = 3;
    static constexpr size_t where_child_index = 4;
    static constexpr size_t group_by_child_index = 5;
    static constexpr size_t having_child_index = 6;
    static constexpr size_t window_child_index = 7;
    static constexpr size_t order_by_child_index = 8;
    static constexpr size_t interpolate_child_index = 9;
    static constexpr size_t limit_by_limit_child_index = 10;
    static constexpr size_t limit_by_offset_child_index = 11;
    static constexpr size_t limit_by_child_index = 12;
    static constexpr size_t limit_child_index = 13;
    static constexpr size_t offset_child_index = 14;
    static constexpr size_t children_size = offset_child_index + 1;
};

}
