#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class InterpreterCreateSQLClusterQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateSQLClusterQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_)
        , query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

class InterpreterAlterSQLClusterQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterAlterSQLClusterQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_)
        , query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

class InterpreterDropSQLClusterQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterDropSQLClusterQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_)
        , query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterCreateSQLClusterQuery(InterpreterFactory & factory);
void registerInterpreterAlterSQLClusterQuery(InterpreterFactory & factory);
void registerInterpreterDropSQLClusterQuery(InterpreterFactory & factory);

}
