#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses a string like this:
  * {role|CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {role|CURRENT_USER} [,...]
  */
class ParserRolesOrUsersSet : public IParserBase
{
public:
    ParserRolesOrUsersSet & allowAll(bool allow_all_ = true) { allow_all = allow_all_; return *this; }
    ParserRolesOrUsersSet & allowUserNames(bool allow_user_names_ = true) { allow_user_names = allow_user_names_; return *this; }
    ParserRolesOrUsersSet & allowRoleNames(bool allow_role_names_ = true) { allow_role_names = allow_role_names_; return *this; }
    ParserRolesOrUsersSet & allowCurrentUser(bool allow_current_user_ = true) { allow_current_user = allow_current_user_; return *this; }
    ParserRolesOrUsersSet & useIDMode(bool id_mode_ = true) { id_mode = id_mode_; return *this; }

protected:
    const char * getName() const override { return "RolesOrUsersSet"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_all = false;
    bool allow_user_names = false;
    bool allow_role_names = false;
    bool allow_current_user = false;
    bool id_mode = false;
};

}
