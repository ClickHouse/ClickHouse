#include <Analyzer/ListNode.h>

#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTExpressionList.h>

namespace DB
{

void ListNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "LIST id: " << format_state.getNodeId(this);

    size_t children_size = children.size();
    buffer << ", nodes: " << children_size << '\n';

    for (size_t i = 0; i < children_size; ++i)
    {
        const auto & node = children[i];
        node->dumpTreeImpl(buffer, format_state, indent + 2);

        if (i + 1 != children_size)
            buffer << '\n';
    }
}

String ListNode::getName() const
{
    if (children.empty())
        return "";

    std::string result;
    for (const auto & node : children)
    {
        result += node->getName();
        result += ", ";
    }

    result.pop_back();
    result.pop_back();

    return result;
}

bool ListNode::isEqualImpl(const IQueryTreeNode &) const
{
    /// No state
    return true;
}

void ListNode::updateTreeHashImpl(HashState &) const
{
    /// No state
}

ASTPtr ListNode::toASTImpl() const
{
    auto expression_list_ast = std::make_shared<ASTExpressionList>();

    size_t children_size = children.size();
    expression_list_ast->children.resize(children_size);

    for (size_t i = 0; i < children_size; ++i)
        expression_list_ast->children[i] = children[i]->toAST();

    return expression_list_ast;
}

QueryTreeNodePtr ListNode::cloneImpl() const
{
    return std::make_shared<ListNode>();
}

}
