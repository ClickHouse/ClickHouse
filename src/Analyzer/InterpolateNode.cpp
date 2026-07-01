#include <Analyzer/InterpolateNode.h>

#include <Common/SipHash.h>
#include <Common/assert_cast.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Parsers/ASTInterpolateElement.h>

namespace DB
{

InterpolateNode::InterpolateNode(std::shared_ptr<IdentifierNode> expression_, QueryTreeNodePtr interpolate_expression_)
    : IQueryTreeNode(children_size)
{
    if (expression_)
    {
        expression_name = expression_->getIdentifier().getFullName();
        /// Capture the original double-quote bit now: once the child is resolved into a ColumnNode
        /// it no longer carries `quote_styles`, so a later `toASTImpl` cannot recover it.
        const auto & quote_styles = expression_->getQuoteStyles();
        expression_is_double_quoted
            = !quote_styles.empty() && quote_styles.front() == IdentifierQuoteStyle::DoubleQuote;
    }

    children[expression_child_index] = std::move(expression_);
    children[interpolate_expression_child_index] = std::move(interpolate_expression_);
}

void InterpolateNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "INTERPOLATE id: " << format_state.getNodeId(this);

    buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION " << expression_name << " \n";
    getExpression()->dumpTreeImpl(buffer, format_state, indent + 4);

    buffer << '\n' << std::string(indent + 2, ' ') << "INTERPOLATE_EXPRESSION\n";
    getInterpolateExpression()->dumpTreeImpl(buffer, format_state, indent + 4);
}

bool InterpolateNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    /// `expression_name` and its quote flag are semantic state: after analysis the expression
    /// child no longer carries the original target spelling, so two INTERPOLATE nodes that
    /// differ only in `"x"` vs `x` (case-sensitivity in `standard` mode) must not compare equal.
    const auto & rhs_typed = assert_cast<const InterpolateNode &>(rhs);
    return expression_name == rhs_typed.expression_name
        && expression_is_double_quoted == rhs_typed.expression_is_double_quoted;
}

void InterpolateNode::updateTreeHashImpl(HashState & hash_state, CompareOptions) const
{
    hash_state.update(expression_name.size());
    hash_state.update(expression_name);
    /// Mix in the quote flag only when set so unquoted targets keep their previous hash.
    if (expression_is_double_quoted)
        hash_state.update(true);
}

QueryTreeNodePtr InterpolateNode::cloneImpl() const
{
    auto cloned = std::make_shared<InterpolateNode>(nullptr /*expression*/, nullptr /*interpolate_expression*/);
    cloned->expression_name = expression_name;
    cloned->expression_is_double_quoted = expression_is_double_quoted;
    return cloned;
}

ASTPtr InterpolateNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto result = make_intrusive<ASTInterpolateElement>();

    /// Interpolate parser supports only identifier node.
    /// In case of alias, identifier is replaced to expression, which can't be parsed.
    /// In this case, keep original alias name.
    if (const auto * identifier = getExpression()->as<IdentifierNode>())
        result->column = identifier->toAST(options)->getColumnName();
    else
        result->column = expression_name;
    /// Carry the original double-quote bit from the InterpolateNode itself: the resolved child
    /// is no longer an `IdentifierNode`, so we cannot read it from there.
    result->column_is_double_quoted = expression_is_double_quoted;

    result->children.push_back(getInterpolateExpression()->toAST(options));
    result->expr = result->children.back();

    return result;
}

}
