#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Identifier.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Core/IdentifierName.h>

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
    /// Construct identifier node with identifier, all parts are considered unquoted
    explicit IdentifierNode(Identifier identifier_);

    /// Construct identifier node from a structured name that preserves per-part quoting
    explicit IdentifierNode(IdentifierName identifier_name_);

    /** Construct identifier node with identifier and table expression modifiers
      * when identifier node is part of JOIN TREE.
      *
      * Example: SELECT * FROM test_table SAMPLE 0.1 OFFSET 0.1 FINAL
      */
    explicit IdentifierNode(Identifier identifier_, TableExpressionModifiers table_expression_modifiers_);

    explicit IdentifierNode(IdentifierName identifier_name_, TableExpressionModifiers table_expression_modifiers_);

    /// Get identifier
    const Identifier & getIdentifier() const
    {
        return identifier;
    }

    /// Get structured name with per-part quoting; part spellings always mirror the identifier
    const IdentifierName & getIdentifierName() const
    {
        return identifier_name;
    }

    /// Return true if identifier node has table expression modifiers, false otherwise
    bool hasTableExpressionModifiers() const
    {
        return table_expression_modifiers.has_value();
    }

    /// Get table expression modifiers
    const std::optional<TableExpressionModifiers> & getTableExpressionModifiers() const
    {
        return table_expression_modifiers;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::IDENTIFIER;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    Identifier identifier;
    IdentifierName identifier_name;
    std::optional<TableExpressionModifiers> table_expression_modifiers;

    static constexpr size_t children_size = 0;
};

}
