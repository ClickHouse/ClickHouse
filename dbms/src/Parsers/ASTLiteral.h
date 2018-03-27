#pragma once

#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <Parsers/ASTWithAlias.h>


namespace DB
{

/** Literal (atomic) - number, string, NULL
  */
class ASTLiteral : public ASTWithAlias
{
public:
    Field value;

    ASTLiteral(const Field & value_) : value(value_) {}

    /** Get the text that identifies this element. */
    String getID() const override { return "Literal_" + applyVisitor(FieldVisitorDump(), value); }

    ASTPtr clone() const override { return std::make_shared<ASTLiteral>(*this); }

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << applyVisitor(FieldVisitorToString(), value);
    }

    String getColumnNameImpl() const override;
};

}
