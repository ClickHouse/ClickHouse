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

    /// Canonicalize the stored target spelling after resolution, so downstream name matching
    /// (projection-based pruning, the planner's actions-DAG alias) uses the canonical column
    /// name even when a folded `standard`-mode lookup resolved a differently-cased target.
    void setExpressionName(std::string name) { expression_name = std::move(name); }

    /// Original target spelling with its quote structure. Captured at construction; the
    /// resolved child is no longer an IdentifierNode, so it cannot be recovered later.
    const IdentifierName & getExpressionIdentifierName() const { return expression_identifier_name; }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & hash_state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

    /// Initial name from column identifier, rewritten to the canonical column spelling
    /// when a folded `standard`-mode lookup resolves the target.
    std::string expression_name;
    /// Original spelling with per-part quoting, for pin checks and AST round trips.
    IdentifierName expression_identifier_name;

private:
    static constexpr size_t expression_child_index = 0;
    static constexpr size_t interpolate_expression_child_index = 1;
    static constexpr size_t children_size = interpolate_expression_child_index + 1;
};

}
