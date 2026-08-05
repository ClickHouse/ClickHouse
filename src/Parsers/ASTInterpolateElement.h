#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

class ASTInterpolateElement : public IAST
{
public:
    String column;
    ASTPtr expr;

    String getID(char delim) const override { return String("InterpolateElement") + delim + "(column " + column + ")"; }

    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
