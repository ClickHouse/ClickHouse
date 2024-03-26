#pragma once

#include <Parsers/ASTWithAlias.h>


namespace DB
{

/// Parameter in query with name and type of substitution ({name:type}).
/// Example: SELECT * FROM table WHERE id = {pid:UInt16}.
class ASTQueryParameter : public ASTWithAlias
{
public:
    String name;
    String type;

    ASTQueryParameter(const String & name_, const String & type_) : name(name_), type(type_) {}

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return String("QueryParameter") + delim + name + ':' + type; }

    ASTPtr clone() const override { return std::make_shared<ASTQueryParameter>(*this); }

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

}
