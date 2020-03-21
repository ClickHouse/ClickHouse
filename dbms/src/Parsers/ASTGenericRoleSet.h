#pragma once

#include <Parsers/IAST.h>
#include <Access/Quota.h>


namespace DB
{
/// Represents a set of users/roles like
/// {user_name | role_name | CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
class ASTGenericRoleSet : public IAST
{
public:
    Strings names;
    bool current_user = false;
    bool all = false;
    Strings except_names;
    bool except_current_user = false;

    bool empty() const { return names.empty() && !current_user && !all; }

    String getID(char) const override { return "GenericRoleSet"; }
    ASTPtr clone() const override { return std::make_shared<ASTGenericRoleSet>(*this); }
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
