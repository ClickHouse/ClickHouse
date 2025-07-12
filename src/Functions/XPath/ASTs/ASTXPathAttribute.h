#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTXPathAttribute : public IAST
{
public:
    String getID(char) const override { return "ASTXPathAttribute"; }

    ASTPtr clone() const override { return std::make_shared<ASTXPathAttribute>(*this); }

    String name;
    String value;
};

}
