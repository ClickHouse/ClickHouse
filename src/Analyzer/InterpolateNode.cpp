#include <Analyzer/InterpolateNode.h>

#include <Common/SipHash.h>
#include <Common/assert_cast.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Parsers/ASTInterpolateElement.h>

namespace DB
{

InterpolateNode::InterpolateNode(std::shared_ptr<IdentifierNode> expression_, QueryTreeNodePtr interpolate_expression_)
    : ITableExpressionNode(children_size)
{
    if (expression_)
        expression_name = expression_->getIdentifier().getFullName();

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
    return projection_column_indexes == assert_cast<const InterpolateNode &>(rhs).projection_column_indexes;
}

void InterpolateNode::updateTreeHashImpl(HashState & hash_state, CompareOptions) const
{
    if (projection_column_indexes.empty())
        return;

    hash_state.update(projection_column_indexes.size());
    for (size_t index : projection_column_indexes)
        hash_state.update(index);
}

QueryTreeNodePtr InterpolateNode::cloneImpl() const
{
    auto cloned = std::make_shared<InterpolateNode>(nullptr /*expression*/, nullptr /*interpolate_expression*/);
    cloned->expression_name = expression_name;
    cloned->projection_column_indexes = projection_column_indexes;
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

    result->children.push_back(getInterpolateExpression()->toAST(options));
    result->expr = result->children.back();

    return result;
}

}
