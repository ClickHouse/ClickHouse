#include <Analyzer/ListNode.h>

#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTExpressionList.h>

namespace DB
{

ListNode::ListNode()
    : IQueryTreeNode(0 /*children_size*/)
{}

ListNode::ListNode(QueryTreeNodes nodes)
    : IQueryTreeNode(0 /*children_size*/)
{
    children = std::move(nodes);
}

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

bool ListNode::isEqualImpl(const IQueryTreeNode &, CompareOptions) const
{
    /// No state
    return true;
}

void ListNode::updateTreeHashImpl(HashState &, CompareOptions) const
{
    /// No state
}

QueryTreeNodePtr ListNode::cloneImpl() const
{
    return std::make_shared<ListNode>();
}

ASTPtr ListNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto expression_list_ast = std::make_shared<ASTExpressionList>();

    size_t children_size = children.size();
    expression_list_ast->children.resize(children_size);

    for (size_t i = 0; i < children_size; ++i)
        expression_list_ast->children[i] = children[i]->toAST(options);

    return expression_list_ast;
}

}
