#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;

class InterpreterShowAccessEntitiesQuery : public IInterpreter
{
public:
    InterpreterShowAccessEntitiesQuery(const ASTPtr & query_ptr_, Context & context_);

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    String getRewrittenQuery() const;

    ASTPtr query_ptr;
    Context & context;
};

}
