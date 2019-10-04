#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class User2;
class ASTCreateUserQuery;


class InterpreterCreateUserQuery : public IInterpreter
{
public:
    InterpreterCreateUserQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}
    BlockIO execute() override;

protected:
    void extractUserOptionsFromQuery(User2 & user, const ASTCreateUserQuery & query) const;
    void extractAuthenticationFromQuery(User2 & user, const ASTCreateUserQuery & query) const;
    void extractAllowedHostsFromQuery(User2 & user, const ASTCreateUserQuery & query) const;
    void extractDefaultRolesFromQuery(User2 & user, const ASTCreateUserQuery & query) const;
    void extractSettingsFromQuery(User2 & user, const ASTCreateUserQuery & query) const;
    void extractAccountLockFromQuery(User2 & user, const ASTCreateUserQuery & query) const;

private:
    ASTPtr query_ptr;
    Context & context;
};
}
