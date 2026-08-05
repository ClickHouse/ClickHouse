#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTCreateUserQuery;
struct User;

class InterpreterCreateUserQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateUserQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    static void updateUserFromQuery(
        User & user,
        const ASTCreateUserQuery & query,
        bool allow_no_password,
        bool allow_plaintext_password,
        std::size_t max_number_of_authentication_methods);

private:
    ASTPtr query_ptr;
};

}
