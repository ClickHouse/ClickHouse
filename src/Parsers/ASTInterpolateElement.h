#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTInterpolateElement : public IAST
{
public:
    ASTPtr column;
    ASTPtr expr;

    String getID(char) const override { return "InterpolateElement"; }

    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTInterpolateElement>(*this);
        clone->cloneChildren();
        return clone;
    }


protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
