#pragma once

#include <Functions/XPath/ASTs/ASTXPathQuery.h>
#include <Parsers/IAST.h>

namespace DB
{

class ASTXPath : public IAST
{
public:
    String getID(char) const override { return "ASTXPath"; }

    ASTPtr clone() const override { return std::make_shared<ASTXPath>(*this); }

    ASTXPathQuery * xpath_query;
};

}
