#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTCreateUserQuery;
struct User;

class InterpreterCreateUserQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateUserQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    static void updateUserFromQuery(User & user, const ASTCreateUserQuery & query);

private:
    ASTPtr query_ptr;
};

}
