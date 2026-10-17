#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/// A named argument of a function call: `f(name => value)`.
class ASTNamedArgument : public IAST
{
public:
    String name;

    ASTPtr value() const { return children.at(0); }

    String getID(char delim) const override { return "NamedArgument" + (delim + name); }

    ASTPtr clone() const override;

    void appendColumnName(WriteBuffer & ostr) const override;
    void appendColumnNameWithoutAlias(WriteBuffer & ostr) const override;

    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
