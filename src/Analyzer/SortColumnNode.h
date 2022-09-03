#pragma once

#include <Columns/Collator.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>

namespace DB
{

/** Sort column node represents sort column descripion that is part of ORDER BY in query tree.
  * Example: SELECT * FROM test_table ORDER BY sort_column_1, sort_column_2;
  * Sort column optionally contain collation, fill from, fill to, and fill step.
  */
class SortColumnNode;
using SortColumnNodePtr = std::shared_ptr<SortColumnNode>;

enum class SortDirection
{
    ASCENDING = 0,
    DESCENDING = 1
};

const char * toString(SortDirection sort_direction);

class SortColumnNode final : public IQueryTreeNode
{
public:
    /// Initialize sort column node with sort expression
    explicit SortColumnNode(QueryTreeNodePtr expression_,
        SortDirection sort_direction_ = SortDirection::ASCENDING,
        std::optional<SortDirection> nulls_sort_direction_ = {},
        std::shared_ptr<Collator> collator_ = nullptr,
        bool with_fill = false);

    /// Get sort expression
    const QueryTreeNodePtr & getExpression() const
    {
        return children[sort_expression_child_index];
    }

    /// Get sort expression
    QueryTreeNodePtr & getExpression()
    {
        return children[sort_expression_child_index];
    }

    /// Has with fill
    bool withFill() const
    {
        return with_fill;
    }

    /// Has fill from
    bool hasFillFrom() const
    {
        return children[fill_from_child_index] != nullptr;
    }

    /// Get fill from
    const QueryTreeNodePtr & getFillFrom() const
    {
        return children[fill_from_child_index];
    }

    /// Get fill from
    QueryTreeNodePtr & getFillFrom()
    {
        return children[fill_from_child_index];
    }

    /// Has fill to
    bool hasFillTo() const
    {
        return children[fill_to_child_index] != nullptr;
    }

    /// Get fill to
    const QueryTreeNodePtr & getFillTo() const
    {
        return children[fill_to_child_index];
    }

    /// Get fill to
    QueryTreeNodePtr & getFillTo()
    {
        return children[fill_to_child_index];
    }

    bool hasFillStep() const
    {
        return children[fill_step_child_index] != nullptr;
    }

    /// Get fill step
    const QueryTreeNodePtr & getFillStep() const
    {
        return children[fill_step_child_index];
    }

    /// Get fill to
    QueryTreeNodePtr & getFillStep()
    {
        return children[fill_step_child_index];
    }

    /// Get collator
    const std::shared_ptr<Collator> & getCollator() const
    {
        return collator;
    }

    SortDirection getSortDirection() const
    {
        return sort_direction;
    }

    std::optional<SortDirection> getNullsSortDirection() const
    {
        return nulls_sort_direction;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::SORT_COLUMN;
    }

    String getName() const override;

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState & hash_state) const override;

protected:
    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

private:
    static constexpr size_t sort_expression_child_index = 0;
    static constexpr size_t fill_from_child_index = 1;
    static constexpr size_t fill_to_child_index = 2;
    static constexpr size_t fill_step_child_index = 3;
    static constexpr size_t children_size = fill_step_child_index + 1;

    SortDirection sort_direction = SortDirection::ASCENDING;
    std::optional<SortDirection> nulls_sort_direction;
    std::shared_ptr<Collator> collator;
    bool with_fill = false;
};

}
