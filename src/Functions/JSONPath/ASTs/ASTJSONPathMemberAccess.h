#pragma once

#include <Parsers/IAST.h>

namespace DB
{
class ASTJSONPathMemberAccess : public IAST
{
public:
    String getID(char) const override { return "ASTJSONPathMemberAccess"; }

    ASTPtr clone() const override { return std::make_shared<ASTJSONPathMemberAccess>(*this); }

public:
    /// Member name to lookup in json document (in path: $.some_key.another_key. ...)
    String member_name;
};

}
