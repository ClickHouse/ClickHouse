#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTSetRoleQuery;
class AccessControl;
struct RolesOrUsersSet;
struct User;

/// A role stops or starts being default without naming any setting, but that decides which of the settings
/// the user was granted apply to it. Throws if `allow_feature_tier` forbids changing one of them.
void checkSettingsOfDefaultRolesChange(
    const ContextMutablePtr & context, const AccessControl & access_control, const User & user, const RolesOrUsersSet & new_default_roles);

class InterpreterSetRoleQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterSetRoleQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    static void updateUserSetDefaultRoles(User & user, const RolesOrUsersSet & roles_from_query);

private:
    void setRole(const ASTSetRoleQuery & query);
    void setDefaultRole(const ASTSetRoleQuery & query);

    ASTPtr query_ptr;
};

}
