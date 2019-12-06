#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;

class InterpreterShowQuotasQuery : public IInterpreter
{
public:
    InterpreterShowQuotasQuery(const ASTPtr & query_ptr_, Context & context_);

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    ASTPtr query_ptr;
    Context & context;

    String getRewrittenQuery();
};

}
