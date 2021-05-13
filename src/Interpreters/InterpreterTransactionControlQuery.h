#pragma once
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class InterpreterTransactionControlQuery : public IInterpreter, WithContext
{
public:
    InterpreterTransactionControlQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_)
    , query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }
private:
    BlockIO executeBegin(ContextPtr session_context);
    BlockIO executeCommit(ContextPtr session_context);
    BlockIO executeRollback(ContextPtr session_context);

private:
    ASTPtr query_ptr;
};

}
