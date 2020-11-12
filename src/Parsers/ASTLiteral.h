#pragma once

#include <Core/Field.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/TokenIterator.h>
#include <Common/FieldVisitors.h>

#include <optional>


namespace DB
{

/// Literal (atomic) - number, string, NULL
class ASTLiteral : public ASTWithAlias
{
public:
    explicit ASTLiteral(Field && value_) : value(value_) {}
    explicit ASTLiteral(const Field & value_) : value(value_) {}

    Field value;

    /// For ConstantExpressionTemplate
    std::optional<TokenIterator> begin;
    std::optional<TokenIterator> end;

    /*
     * The name of the column corresponding to this literal. Only used to
     * disambiguate the literal columns with the same display name that are
     * created at the expression analyzer stage. In the future, we might want to
     * have a full separation between display names and column identifiers. For
     * now, this field is effectively just some private EA data.
     */
    String unique_column_name;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "Literal" + (delim + applyVisitor(FieldVisitorDump(), value)); }

    ASTPtr clone() const override { return std::make_shared<ASTLiteral>(*this); }

    void updateTreeHashImpl(SipHash & hash_state) const override;

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

}
