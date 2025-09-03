#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTInterpolateElement : public IAST
{
public:
    String column;
    ASTPtr expr;

    String getID(char delim) const override { return String("InterpolateElement") + delim + "(column " + column + ")"; }

    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
