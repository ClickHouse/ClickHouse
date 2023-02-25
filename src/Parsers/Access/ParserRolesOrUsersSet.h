#pragma once

#include <Core/Types.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses a string like this:
  * {user_name [AS USER] | role_name [AS ROLE] | user_and_role_name AS BOTH | CURRENT_USER | ALL | NONE} [,...]
  * [EXCEPT {user_name [AS USER] | role_name [AS ROLE] | user_and_role_name AS BOTH | CURRENT_USER | ALL | NONE} [,...]]
  */
class ParserRolesOrUsersSet : public IParserBase
{
public:
    ParserRolesOrUsersSet & allowAll(bool allow_all_ = true) { allow_all = allow_all_; return *this; }
    ParserRolesOrUsersSet & allowAny(bool allow_any_ = true) { allow_any = allow_any_; return *this; }
    ParserRolesOrUsersSet & allowUsers(bool allow_users_ = true) { allow_users = allow_users_; return *this; }
    ParserRolesOrUsersSet & allowCurrentUser(bool allow_current_user_ = true) { allow_current_user = allow_current_user_; return *this; }
    ParserRolesOrUsersSet & allowRoles(bool allow_roles_ = true) { allow_roles = allow_roles_; return *this; }
    ParserRolesOrUsersSet & useIDMode(bool id_mode_ = true) { id_mode = id_mode_; return *this; }

protected:
    const char * getName() const override { return "RolesOrUsersSet"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_all = false;
    bool allow_any = false;
    bool allow_users = false;
    bool allow_current_user = false;
    bool allow_roles = false;
    bool id_mode = false;
    bool parseBeforeExcept(
        IParserBase::Pos & pos,
        Expected & expected,
        bool & all,
        Strings & names,
        ASTRolesOrUsersSet::NameFilters & names_filters,
        bool & current_user) const;
    bool parseExceptAndAfterExcept(
        IParserBase::Pos & pos,
        Expected & expected,
        Strings & except_names,
        ASTRolesOrUsersSet::NameFilters & except_names_filters,
        bool & except_current_user) const;
};

}
