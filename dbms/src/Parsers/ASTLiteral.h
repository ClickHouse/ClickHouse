#pragma once

#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/TokenIterator.h>
#include <optional>


namespace DB
{

/** Literal (atomic) - number, string, NULL
  */
class ASTLiteral : public ASTWithAlias
{
public:
    Field value;

    /// For ConstantExpressionTemplate
    std::optional<TokenIterator> begin;
    std::optional<TokenIterator> end;

    ASTLiteral(const Field & value_) : value(value_) {}

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "Literal" + (delim + applyVisitor(FieldVisitorDump(), value)); }

    ASTPtr clone() const override { return std::make_shared<ASTLiteral>(*this); }

    void updateTreeHashImpl(SipHash & hash_state) const override;

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << applyVisitor(FieldVisitorToString(), value);
    }

    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

}
