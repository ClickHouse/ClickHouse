#pragma once
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class InterpreterTransactionControlQuery : public IInterpreter
{
public:
    InterpreterTransactionControlQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : query_context(context_)
    , query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }
private:
    BlockIO executeBegin(ContextMutablePtr session_context);
    BlockIO executeCommit(ContextMutablePtr session_context);
    BlockIO executeRollback(ContextMutablePtr session_context);

private:
    ContextMutablePtr query_context;
    ASTPtr query_ptr;
};

}
