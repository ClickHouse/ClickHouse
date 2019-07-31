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
    bool password{false};

    ASTLiteral(const Field & value_, bool password_ = false) : value(value_), password(password_) {}

    /** Get the text that identifies this element. */
    String getID(char delim) const override
    {
        return "Literal" + (delim + (password ? "******" : applyVisitor(FieldVisitorDump(), value)));
    }

    ASTPtr clone() const override { return std::make_shared<ASTLiteral>(*this); }

    void updateTreeHashImpl(SipHash & hash_state) const override;

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << (password ? "******" : applyVisitor(FieldVisitorToString(), value));
    }

    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

}
