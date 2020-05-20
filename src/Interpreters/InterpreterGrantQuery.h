#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Core/UUID.h>


namespace DB
{
class ASTGrantQuery;
struct User;
struct Role;


class InterpreterGrantQuery : public IInterpreter
{
public:
    InterpreterGrantQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    static void updateUserFromQuery(User & user, const ASTGrantQuery & query);
    static void updateRoleFromQuery(Role & role, const ASTGrantQuery & query);

private:
    ASTPtr query_ptr;
    Context & context;
};
}
