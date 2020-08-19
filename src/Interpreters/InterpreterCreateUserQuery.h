#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class ASTCreateUserQuery;
struct User;


class InterpreterCreateUserQuery : public IInterpreter
{
public:
    InterpreterCreateUserQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    static void updateUserFromQuery(User & user, const ASTCreateUserQuery & query);

private:
    ASTPtr query_ptr;
    Context & context;
};
}
