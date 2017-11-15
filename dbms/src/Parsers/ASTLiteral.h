#pragma once

#include <Core/Field.h>
#include <Core/FieldVisitors.h>
#include <Parsers/ASTWithAlias.h>


namespace DB
{

/** Literal (atomic) - number, string, NULL
  */
class ASTLiteral : public ASTWithAlias
{
public:
    Field value;

    ASTLiteral() = default;
    ASTLiteral(const StringRange range_, const Field & value_) : ASTWithAlias(range_), value(value_) {}

    /** Get the text that identifies this element. */
    String getID() const override { return "Literal_" + applyVisitor(FieldVisitorDump(), value); }

    ASTPtr clone() const override { return std::make_shared<ASTLiteral>(*this); }

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << applyVisitor(FieldVisitorToString(), value);
    }
    String getColumnNameImpl() const override;
};

}
