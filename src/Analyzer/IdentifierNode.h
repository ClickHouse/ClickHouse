#pragma once

#include <vector>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Identifier.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Parsers/ASTIdentifier.h>

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
    explicit IdentifierNode(Identifier identifier_);

    explicit IdentifierNode(Identifier identifier_, std::vector<IdentifierQuoteStyle> quote_styles_);

    /** Construct identifier node with identifier and table expression modifiers
      * when identifier node is part of JOIN TREE.
      *
      * Example: SELECT * FROM test_table SAMPLE 0.1 OFFSET 0.1 FINAL
      */
    explicit IdentifierNode(Identifier identifier_, TableExpressionModifiers table_expression_modifiers_);

    /// Get identifier
    const Identifier & getIdentifier() const
    {
        return identifier;
    }

    const std::vector<IdentifierQuoteStyle> & getQuoteStyles() const
    {
        return quote_styles;
    }

    void setQuoteStyles(std::vector<IdentifierQuoteStyle> styles)
    {
        quote_styles = std::move(styles);
    }

    IdentifierQuoteStyle getQuoteStyleAt(size_t index) const
    {
        return index < quote_styles.size() ? quote_styles[index] : IdentifierQuoteStyle::None;
    }

    void setQuoteStyle(IdentifierQuoteStyle style)
    {
        if (quote_styles.empty())
            quote_styles.resize(!identifier.getParts().empty() ? identifier.getParts().size() : 1, IdentifierQuoteStyle::None);
        quote_styles[0] = style;
    }

    bool isPartDoubleQuoted(size_t index) const
    {
        return index < quote_styles.size() && quote_styles[index] == IdentifierQuoteStyle::DoubleQuote;
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
    std::optional<TableExpressionModifiers> table_expression_modifiers;
    std::vector<IdentifierQuoteStyle> quote_styles;  /// One per identifier part, for SQL standard case-sensitivity

    static constexpr size_t children_size = 0;
};

}
