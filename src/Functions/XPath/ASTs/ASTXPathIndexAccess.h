#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTXPathIndexAccess : public IAST
{
public:
    String getID(char) const override { return "ASTXPathNthMemberAccess"; }

    ASTPtr clone() const override { return std::make_shared<ASTXPathIndexAccess>(*this); }

    UInt64 index;
};

}
