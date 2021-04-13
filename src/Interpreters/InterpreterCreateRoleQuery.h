#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTCreateRoleQuery;
struct Role;

class InterpreterCreateRoleQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateRoleQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    static void updateRoleFromQuery(Role & role, const ASTCreateRoleQuery & query);

private:
    ASTPtr query_ptr;
};

}
