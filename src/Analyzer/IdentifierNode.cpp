#include <Analyzer/IdentifierNode.h>

#include <Common/SipHash.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Parsers/ASTIdentifier.h>

namespace DB
{

void IdentifierNode::dumpTree(WriteBuffer & buffer, size_t indent) const
{
    buffer << std::string(indent, ' ') << "IDENTIFIER ";
    writePointerHex(this, buffer);
    buffer << ' ' << identifier.getFullName();
}

bool IdentifierNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const IdentifierNode &>(rhs);
    return identifier == rhs_typed.identifier;
}

void IdentifierNode::updateTreeHashImpl(HashState & state) const
{
    const auto & identifier_name = identifier.getFullName();
    state.update(identifier_name.size());
    state.update(identifier_name);
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
