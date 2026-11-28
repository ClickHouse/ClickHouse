#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class InterpreterShowAccessEntitiesQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterShowAccessEntitiesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    [[nodiscard]] bool ignoreQuota() const override { return true; }
    [[nodiscard]] bool ignoreLimits() const override { return true; }

private:
    [[nodiscard]] String getRewrittenQuery() const;

    ASTPtr query_ptr;
};

}
