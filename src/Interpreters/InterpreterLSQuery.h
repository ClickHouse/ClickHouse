#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class InterpreterLSQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterLSQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    /// We ignore the quota and limits here because execute() will rewrite an ls query as a SELECT query and then
    /// the SELECT query will check the quota and limits.
    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    ASTPtr query_ptr;
};

}

