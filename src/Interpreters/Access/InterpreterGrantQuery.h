#pragma once

#include <Core/UUID.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTGrantQuery;
struct User;
struct Role;

class InterpreterGrantQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterGrantQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    static void updateUserFromQuery(User & user, const ASTGrantQuery & query);
    static void updateRoleFromQuery(Role & role, const ASTGrantQuery & query);
    void extendQueryLogElemImpl(QueryLogElement &, const ASTPtr &, ContextPtr) const override;

private:
    ASTPtr query_ptr;
};

}
