#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class ASTCreateUserQuery;
struct GenericRoleSet;
struct User;


class InterpreterCreateUserQuery : public IInterpreter
{
public:
    InterpreterCreateUserQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    void updateUserFromQuery(User & user, const ASTCreateUserQuery & query, const GenericRoleSet * default_roles_from_query);

    ASTPtr query_ptr;
    Context & context;
};
}
