#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Role;
class User2;
class ASTCreateUserQuery;
class ASTAuthentication;
class ASTAllowedHosts;
class ASTDefaultRoles;


class InterpreterCreateRoleQuery : public IInterpreter
{
public:
    InterpreterCreateRoleQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;
};


class InterpreterCreateUserQuery : public IInterpreter
{
public:
    InterpreterCreateUserQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}
    BlockIO execute() override;

protected:
    void changeUser(User2 & user, const ASTCreateUserQuery & ast) const;
    void changeUser(User2 & user, const ASTAuthentication & ast) const;
    void changeUser(User2 & user, const ASTAllowedHosts & ast) const;
    void changeUser(User2 & user, const ASTDefaultRoles & ast) const;

private:
    ASTPtr query_ptr;
    Context & context;
};
}
