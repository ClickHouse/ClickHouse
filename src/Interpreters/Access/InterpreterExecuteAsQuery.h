#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class ASTExecuteAsQuery;
struct RolesOrUsersSet;
struct User;

class InterpreterExecuteAsQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterExecuteAsQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;


private:
    void setImpersonateUser(const ASTExecuteAsQuery & query);
    ASTPtr query_ptr;
};

}
