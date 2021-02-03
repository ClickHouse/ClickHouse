#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class ASTCreateRoleQuery;
struct Role;


class InterpreterCreateRoleQuery : public IInterpreter
{
public:
    InterpreterCreateRoleQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    static void updateRoleFromQuery(Role & role, const ASTCreateRoleQuery & query);

private:
    ASTPtr query_ptr;
    Context & context;
};
}
