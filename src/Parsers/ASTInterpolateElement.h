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

    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTInterpolateElement>(*this);
        clone->cloneChildren();
        clone->expr = clone->expr->clone();
        return clone;
    }


protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
