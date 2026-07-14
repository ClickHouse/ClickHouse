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
    , identifier_name(identifier.getParts())
{}

IdentifierNode::IdentifierNode(IdentifierName identifier_name_)
    : IQueryTreeNode(children_size)
    , identifier(identifier_name_.spellings())
    , identifier_name(std::move(identifier_name_))
{}

IdentifierNode::IdentifierNode(Identifier identifier_, TableExpressionModifiers table_expression_modifiers_)
    : IQueryTreeNode(children_size)
    , identifier(std::move(identifier_))
    , identifier_name(identifier.getParts())
    , table_expression_modifiers(std::move(table_expression_modifiers_))
{}

IdentifierNode::IdentifierNode(IdentifierName identifier_name_, TableExpressionModifiers table_expression_modifiers_)
    : IQueryTreeNode(children_size)
    , identifier(identifier_name_.spellings())
    , identifier_name(std::move(identifier_name_))
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
    if (identifier != rhs_typed.identifier || table_expression_modifiers != rhs_typed.table_expression_modifiers)
        return false;

    /// Quote structure is compared only when a double-quoted part pins exact matching,
    /// so all-unquoted identifiers compare exactly as before.
    if (!identifier_name.anyPartDoubleQuoted() && !rhs_typed.identifier_name.anyPartDoubleQuoted())
        return true;

    if (identifier_name.size() != rhs_typed.identifier_name.size())
        return false;

    for (size_t i = 0; i < identifier_name.size(); ++i)
    {
        bool lhs_double_quoted = identifier_name[i].quote == IdentifierPartQuote::DoubleQuoted;
        bool rhs_double_quoted = rhs_typed.identifier_name[i].quote == IdentifierPartQuote::DoubleQuoted;
        if (lhs_double_quoted != rhs_double_quoted)
            return false;
    }

    return true;
}

void IdentifierNode::updateTreeHashImpl(HashState & state, CompareOptions) const
{
    const auto & full_name = identifier.getFullName();
    state.update(full_name.size());
    state.update(full_name);

    /// Mix quote flags only when a double-quoted part is present, to keep the hash
    /// of all-unquoted identifiers unchanged.
    if (identifier_name.anyPartDoubleQuoted())
    {
        for (const auto & part : identifier_name)
            state.update(static_cast<UInt8>(part.quote == IdentifierPartQuote::DoubleQuoted));
    }

    if (table_expression_modifiers)
        table_expression_modifiers->updateTreeHash(state);
}

QueryTreeNodePtr IdentifierNode::cloneImpl() const
{
    auto clone_identifier_node = std::make_shared<IdentifierNode>(identifier_name);
    clone_identifier_node->table_expression_modifiers = table_expression_modifiers;
    return clone_identifier_node;
}

ASTPtr IdentifierNode::toASTImpl(const ConvertToASTOptions & /* options */) const
{
    return make_intrusive<ASTIdentifier>(identifier_name);
}

}
