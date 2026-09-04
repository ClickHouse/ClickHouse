#pragma once

#include <Access/Common/AccessRightsElement.h>
#include <Parsers/IAST.h>
#include <Parsers/Access/ASTUserNameWithHost.h>


namespace DB
{

using Strings = std::vector<String>;
using ASTUserNamesWithHostPtr = boost::intrusive_ptr<ASTUserNamesWithHost>;

/// Represents a set of users/roles like
/// {user_name | role_name | CURRENT_USER | ALL | NONE} [,...]
/// [EXCEPT {user_name | role_name | CURRENT_USER | ALL | NONE} [,...]]
class ASTRolesOrUsersSet : public IAST
{
public:
    bool all = false;
    ASTUserNamesWithHostPtr names;
    bool current_user = false;
    ASTUserNamesWithHostPtr except_names;
    bool except_current_user = false;

    bool allow_users = true;      /// whether this set can contain names of users
    bool allow_roles = true;      /// whether this set can contain names of roles
    bool id_mode = false;         /// whether this set keep UUIDs instead of names
    bool use_keyword_any = false; /// whether the keyword ANY should be used instead of the keyword ALL

    bool empty() const { return (!names || names->children.empty()) && !current_user && !all; }
    bool hasQueryParameters() const
    {
        return (names && names->hasQueryParameters())
            || (except_names && except_names->hasQueryParameters());
    }
    void replaceCurrentUserTag(const String & current_user_name);
    AccessRightsElements collectRequiredGrants(AccessType access_type) const;

    String getID(char) const override { return "RolesOrUsersSet"; }
    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
