#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTXPathQuery : public IAST
{
public:
    String getID(char) const override { return "ASTXPathQuery"; }

    ASTPtr clone() const override { return std::make_shared<ASTXPathQuery>(*this); }
};

}
