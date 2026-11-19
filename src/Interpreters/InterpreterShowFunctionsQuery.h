#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class Context;

class InterpreterShowFunctionsQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterShowFunctionsQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    [[nodiscard]] bool ignoreQuota() const override { return true; }
    [[nodiscard]] bool ignoreLimits() const override { return true; }

private:
    ASTPtr query_ptr;

    String getRewrittenQuery();
};

}
