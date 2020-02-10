#pragma once

#include <Parsers/IAST.h>
#include <Access/Quota.h>


namespace DB
{
/// {role|CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {role|CURRENT_USER} [,...]
class ASTRoleList : public IAST
{
public:
    Strings names;
    bool current_user = false;
    bool all = false;
    Strings except_names;
    bool except_current_user = false;

    bool empty() const { return names.empty() && !current_user && !all; }

    String getID(char) const override { return "RoleList"; }
    ASTPtr clone() const override { return std::make_shared<ASTRoleList>(*this); }
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
