#pragma once

#include <Parsers/IAST.h>


namespace DB
{
/// Represents a set of users/roles like
/// {user_name | role_name | CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
class ASTRolesOrUsersSet : public IAST
{
public:
    Strings names;
    bool current_user = false;
    bool all = false;
    Strings except_names;
    bool except_current_user = false;

    bool id_mode = false;         /// true if `names` and `except_names` keep UUIDs, not names.
    bool allow_role_names = true; /// true if this set can contain names of roles.
    bool allow_user_names = true; /// true if this set can contain names of users.

    bool empty() const { return names.empty() && !current_user && !all; }
    void replaceCurrentUserTagWithName(const String & current_user_name);

    String getID(char) const override { return "RolesOrUsersSet"; }
    ASTPtr clone() const override { return std::make_shared<ASTRolesOrUsersSet>(*this); }
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
