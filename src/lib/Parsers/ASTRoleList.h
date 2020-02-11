#pragma once

#include <Parsers/IAST.h>
#include <Access/Quota.h>


namespace DB
{
/// {role|CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {role|CURRENT_USER} [,...]
class ASTRoleList : public IAST
{
public:
    Strings roles;
    bool current_user = false;
    bool all_roles = false;
    Strings except_roles;
    bool except_current_user = false;

    bool empty() const { return roles.empty() && !current_user && !all_roles; }

    String getID(char) const override { return "RoleList"; }
    ASTPtr clone() const override { return std::make_shared<ASTRoleList>(*this); }
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
