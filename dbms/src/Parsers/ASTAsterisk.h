#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTAsterisk : public IAST
{
public:
    String getID() const override { return "Asterisk"; }
    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTAsterisk>(*this);
        clone->cloneChildren();
        return std::move(clone);
    }
    String getColumnName() const override { return "*"; }

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << "*";
    }
};

}
