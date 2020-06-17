#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;
class ASTSetRoleQuery;
struct RolesOrUsersSet;
struct User;


class InterpreterSetRoleQuery : public IInterpreter
{
public:
    InterpreterSetRoleQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    static void updateUserSetDefaultRoles(User & user, const RolesOrUsersSet & roles_from_query);

private:
    void setRole(const ASTSetRoleQuery & query);
    void setDefaultRole(const ASTSetRoleQuery & query);

    ASTPtr query_ptr;
    Context & context;
};
}
