#pragma once
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class InterpreterTransactionControlQuery : public IInterpreter
{
public:
    InterpreterTransactionControlQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_)
    , query_context(context_)
    {
    }

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }
private:
    BlockIO executeBegin(Context & context);
    BlockIO executeCommit(Context & context);
    BlockIO executeRollback(Context & context);

private:
    ASTPtr query_ptr;
    Context & query_context;
};

}
