#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTXPathText : public IAST
{
public:
    String getID(char) const override { return "ASTXPathText"; }

    ASTPtr clone() const override { return std::make_shared<ASTXPathText>(*this); }
};

}
