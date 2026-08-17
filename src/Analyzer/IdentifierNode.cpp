#include <Analyzer/IdentifierNode.h>

#include <Common/assert_cast.h>
#include <Common/SipHash.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Parsers/ASTIdentifier.h>

namespace DB
{

IdentifierNode::IdentifierNode(Identifier identifier_)
    : IQueryTreeNode(children_size)
    , identifier(std::move(identifier_))
{}

IdentifierNode::IdentifierNode(Identifier identifier_, TableExpressionModifiers table_expression_modifiers_)
    : IQueryTreeNode(children_size)
    , identifier(std::move(identifier_))
    , table_expression_modifiers(std::move(table_expression_modifiers_))
{}

void IdentifierNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "IDENTIFIER id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", identifier: " << identifier.getFullName();

    if (table_expression_modifiers)
    {
        buffer << ", ";
        table_expression_modifiers->dump(buffer);
    }
}

bool IdentifierNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const IdentifierNode &>(rhs);
    return identifier == rhs_typed.identifier && table_expression_modifiers == rhs_typed.table_expression_modifiers;
}

void IdentifierNode::updateTreeHashImpl(HashState & state, CompareOptions) const
{
    const auto & identifier_name = identifier.getFullName();
    state.update(identifier_name.size());
    state.update(identifier_name);

    if (table_expression_modifiers)
        table_expression_modifiers->updateTreeHash(state);
}

QueryTreeNodePtr IdentifierNode::cloneImpl() const
{
    auto clone_identifier_node = std::make_shared<IdentifierNode>(identifier);
    clone_identifier_node->table_expression_modifiers = table_expression_modifiers;
    return clone_identifier_node;
}

ASTPtr IdentifierNode::toASTImpl(const ConvertToASTOptions & /* options */) const
{
    auto identifier_parts = identifier.getParts();
    return std::make_shared<ASTIdentifier>(std::move(identifier_parts));
}

}
