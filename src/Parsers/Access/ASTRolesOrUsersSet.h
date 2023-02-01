#pragma once

#include <map>

#include <Parsers/IAST.h>


namespace DB
{

using Strings = std::vector<String>;

/// Represents a set of users/roles like
/// {user_name [AS USER] | role_name [AS ROLE] | user_and_role_name AS BOTH | CURRENT_USER | ALL | NONE} [,...]
/// [EXCEPT {user_name [AS USER] | role_name [AS ROLE] | user_and_role_name AS BOTH | CURRENT_USER | ALL | NONE} [,...]]
class ASTRolesOrUsersSet : public IAST
{
public:
    enum struct NameFilter
    {
        ANY = 0,
        USER,
        ROLE,
        BOTH
    };
    using NameFilters = std::map<String, NameFilter>;

    bool all = false;
    Strings names;
    bool current_user = false;
    Strings except_names;
    bool except_current_user = false;

    bool allow_users = true;      /// whether this set can contain names of users
    bool allow_roles = true;      /// whether this set can contain names of roles
    bool id_mode = false;         /// whether this set keep UUIDs instead of names
    bool use_keyword_any = false; /// whether the keyword ANY should be used instead of the keyword ALL

    bool enable_extended_subject_syntax = false;

    bool empty() const { return names.empty() && !current_user && !all; }
    void replaceCurrentUserTag(const String & current_user_name);

    String getID(char) const override { return "RolesOrUsersSet"; }
    ASTPtr clone() const override { return std::make_shared<ASTRolesOrUsersSet>(*this); }
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

    NameFilters names_filters;
    NameFilters except_names_filters;
    NameFilter getNameFilter(const String & name) const;
    NameFilter getExceptNameFilter(const String & name) const;
    NameFilter getFromFilters(const String & name, const NameFilters & filters) const;
};
}
