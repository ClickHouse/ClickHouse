#pragma once

#include <Columns/Collator.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>

namespace DB
{

/** Sort node represents sort description for expression that is part of ORDER BY in query tree.
  * Example: SELECT * FROM test_table ORDER BY sort_column_1, sort_column_2;
  * Sort node optionally contain collation, fill from, fill to, and fill step.
  */
class SortNode;
using SortNodePtr = std::shared_ptr<SortNode>;

enum class SortDirection : uint8_t
{
    ASCENDING = 0,
    DESCENDING = 1
};

const char * toString(SortDirection sort_direction);

class SortNode final : public IQueryTreeNode
{
public:
    /// Initialize sort node with sort expression
    explicit SortNode(QueryTreeNodePtr expression_,
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

    /// Returns true if sort node has with fill, false otherwise
    bool withFill() const
    {
        return with_fill;
    }

    /// Returns true if sort node has fill from, false otherwise
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

    /// Returns true if sort node has fill to, false otherwise
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

    /// Returns true if sort node has fill step, false otherwise
    bool hasFillStep() const
    {
        return children[fill_step_child_index] != nullptr;
    }

    /// Get fill step
    const QueryTreeNodePtr & getFillStep() const
    {
        return children[fill_step_child_index];
    }

    /// Get fill step
    QueryTreeNodePtr & getFillStep()
    {
        return children[fill_step_child_index];
    }

    /// Get collator
    const std::shared_ptr<Collator> & getCollator() const
    {
        return collator;
    }

    /// Get sort direction
    SortDirection getSortDirection() const
    {
        return sort_direction;
    }

    /// Get nulls sort direction
    std::optional<SortDirection> getNullsSortDirection() const
    {
        return nulls_sort_direction;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::SORT;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & hash_state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

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
