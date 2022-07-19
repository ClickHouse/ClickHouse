#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Identifier.h>

namespace DB
{

/** Identifier node represents identifier in query tree.
  * Example: SELECT a FROM test_table.
  * a - is identifier.
  * test_table - is identifier.
  *
  * Identifier resolution must be done during query analysis pass.
  */
class IdentifierNode final : public IQueryTreeNode
{
public:
    /// Construct identifier node with identifier
    explicit IdentifierNode(Identifier identifier_)
        : identifier(std::move(identifier_))
    {}

    /// Get identifier
    const Identifier & getIdentifier() const
    {
        return identifier;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::IDENTIFIER;
    }

    String getName() const override
    {
        return identifier.getFullName();
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState & state) const override;

protected:
    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

private:
    Identifier identifier;
};

}
