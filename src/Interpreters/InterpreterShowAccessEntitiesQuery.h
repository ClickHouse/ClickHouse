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

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    String getRewrittenQuery() const;

    ASTPtr query_ptr;
};

}
