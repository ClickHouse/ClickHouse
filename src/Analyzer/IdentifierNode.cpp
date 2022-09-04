#include <Analyzer/IdentifierNode.h>

#include <Common/SipHash.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Parsers/ASTIdentifier.h>

namespace DB
{

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

bool IdentifierNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const IdentifierNode &>(rhs);

    if (table_expression_modifiers && rhs_typed.table_expression_modifiers && table_expression_modifiers != rhs_typed.table_expression_modifiers)
        return false;
    else if (table_expression_modifiers && !rhs_typed.table_expression_modifiers)
        return false;
    else if (!table_expression_modifiers && rhs_typed.table_expression_modifiers)
        return false;


    return identifier == rhs_typed.identifier;
}

void IdentifierNode::updateTreeHashImpl(HashState & state) const
{
    const auto & identifier_name = identifier.getFullName();
    state.update(identifier_name.size());
    state.update(identifier_name);

    if (table_expression_modifiers)
        table_expression_modifiers->updateTreeHash(state);
}

ASTPtr IdentifierNode::toASTImpl() const
{
    auto identifier_parts = identifier.getParts();
    return std::make_shared<ASTIdentifier>(std::move(identifier_parts));
}

QueryTreeNodePtr IdentifierNode::cloneImpl() const
{
    return std::make_shared<IdentifierNode>(identifier);
}

}
