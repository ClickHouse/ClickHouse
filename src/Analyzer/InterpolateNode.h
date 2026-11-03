#pragma once

#include <Analyzer/IdentifierNode.h>
#include <Analyzer/ListNode.h>

namespace DB
{

/** Interpolate node represents expression interpolation in INTERPOLATE section that is part of ORDER BY section in query tree.
  *
  * Example: SELECT * FROM test_table ORDER BY id WITH FILL INTERPOLATE (value AS value + 1);
  * value - expression to interpolate.
  * value + 1 - interpolate expression.
  */
class InterpolateNode;
using InterpolateNodePtr = std::shared_ptr<InterpolateNode>;

class InterpolateNode final : public IQueryTreeNode
{
public:
    /// Initialize interpolate node with expression and interpolate expression
    explicit InterpolateNode(std::shared_ptr<IdentifierNode> expression_, QueryTreeNodePtr interpolate_expression_);

    /// Get expression to interpolate
    const QueryTreeNodePtr & getExpression() const
    {
        return children[expression_child_index];
    }

    /// Get expression to interpolate
    QueryTreeNodePtr & getExpression()
    {
        return children[expression_child_index];
    }

    /// Get interpolate expression
    const QueryTreeNodePtr & getInterpolateExpression() const
    {
        return children[interpolate_expression_child_index];
    }

    /// Get interpolate expression
    QueryTreeNodePtr & getInterpolateExpression()
    {
        return children[interpolate_expression_child_index];
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::INTERPOLATE;
    }

    const std::string & getExpressionName() const { return expression_name; }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & hash_state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

    /// Initial name from column identifier.
    std::string expression_name;

private:
    static constexpr size_t expression_child_index = 0;
    static constexpr size_t interpolate_expression_child_index = 1;
    static constexpr size_t children_size = interpolate_expression_child_index + 1;
};

}
