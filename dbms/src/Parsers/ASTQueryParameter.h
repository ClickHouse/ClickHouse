#pragma once

#include <Parsers/ASTWithAlias.h>


namespace DB
{

/// Query parameter: name and type.
class ASTQueryParameter : public ASTWithAlias
{
public:
    String name, type;

    ASTQueryParameter(const String & name_, const String & type_) : name(name_), type(type_) {}

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "QueryParameter" + (delim + name + delim + type); }

    ASTPtr clone() const override { return std::make_shared<ASTQueryParameter>(*this); };

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

}
