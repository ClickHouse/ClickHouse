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

    bool ignoreQuota() const override { return ignore_quota; }
    bool ignoreLimits() const override { return ignore_quota; }

private:
    String getRewrittenQuery() const;

    ASTPtr query_ptr;
    Context & context;
    bool ignore_quota = false;
};

}
