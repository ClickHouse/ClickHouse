#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>

namespace DB
{

/** Interpolate column node represents single column interpolation in INTERPOLATE section that part of ORDER BY in query tree.
  * Example: SELECT * FROM test_table ORDER BY id WITH FILL INTERPOLATE value AS value + 1;
  * value - expression to interpolate.
  * value + 1 - interpolate expression.
  */
class InterpolateColumnNode;
using InterpolateColumnNodePtr = std::shared_ptr<InterpolateColumnNode>;

class InterpolateColumnNode final : public IQueryTreeNode
{
public:
    /// Initialize interpolate column node with expression and interpolate expression
    explicit InterpolateColumnNode(QueryTreeNodePtr expression_, QueryTreeNodePtr interpolate_expression_);

    /// Get expression
    const QueryTreeNodePtr & getExpression() const
    {
        return children[expression_child_index];
    }

    /// Get expression
    QueryTreeNodePtr & getExpression()
    {
        return children[expression_child_index];
    }

    /// Get expression
    const QueryTreeNodePtr & getInterpolateExpression() const
    {
        return children[interpolate_expression_child_index];
    }

    /// Get expression
    QueryTreeNodePtr & getInterpolateExpression()
    {
        return children[interpolate_expression_child_index];
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::INTERPOLATE_COLUMN;
    }

    String getName() const override;

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState & hash_state) const override;

protected:
    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

private:
    static constexpr size_t expression_child_index = 0;
    static constexpr size_t interpolate_expression_child_index = 1;
    static constexpr size_t children_size = interpolate_expression_child_index + 1;
};

}
