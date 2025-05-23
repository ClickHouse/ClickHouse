#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTXPathMemberAccess : public IAST
{
public:
    String getID(char) const override { return "ASTXPathMemberAccess"; }

    ASTPtr clone() const override { return std::make_shared<ASTXPathMemberAccess>(*this); }

    String member_name;
    bool skip_ancestors;
};

}
