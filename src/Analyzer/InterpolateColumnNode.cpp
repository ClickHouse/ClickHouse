#include <Analyzer/InterpolateColumnNode.h>

#include <Common/SipHash.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Parsers/ASTInterpolateElement.h>

namespace DB
{

InterpolateColumnNode::InterpolateColumnNode(QueryTreeNodePtr expression_, QueryTreeNodePtr interpolate_expression_)
{
    children.resize(children_size);
    children[expression_child_index] = std::move(expression_);
    children[interpolate_expression_child_index] = std::move(interpolate_expression_);
}

String InterpolateColumnNode::getName() const
{
    String result = getExpression()->getName();
    result += " AS ";
    result += getInterpolateExpression()->getName();

    return result;
}

void InterpolateColumnNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "INTERPOLATE_COLUMN id: " << format_state.getNodeId(this);

    buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION\n";
    getExpression()->dumpTreeImpl(buffer, format_state, indent + 4);

    buffer << '\n' << std::string(indent + 2, ' ') << "INTERPOLATE_EXPRESSION\n";
    getInterpolateExpression()->dumpTreeImpl(buffer, format_state, indent + 4);
}

bool InterpolateColumnNode::isEqualImpl(const IQueryTreeNode &) const
{
    /// No state in interpolate column node
    return true;
}

void InterpolateColumnNode::updateTreeHashImpl(HashState &) const
{
    /// No state in interpolate column node
}

ASTPtr InterpolateColumnNode::toASTImpl() const
{
    auto result = std::make_shared<ASTInterpolateElement>();
    result->column = getExpression()->toAST()->getColumnName();
    result->children.push_back(getInterpolateExpression()->toAST());
    result->expr = result->children.back();

    return result;
}

QueryTreeNodePtr InterpolateColumnNode::cloneImpl() const
{
    return std::make_shared<InterpolateColumnNode>(nullptr /*expression*/, nullptr /*interpolate_expression*/);
}

}
