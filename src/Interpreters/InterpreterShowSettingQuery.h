#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;


/// Returns setting value for specified setting name
class InterpreterShowSettingQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterShowSettingQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    /// Ignore quota and limits here because execute() produces a SELECT query which checks quotas/limits by itself.
    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    ASTPtr query_ptr;

    String getRewrittenQuery();
};


}

