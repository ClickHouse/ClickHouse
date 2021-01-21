#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;

class InterpreterShowPrivilegesQuery : public IInterpreter
{
public:
    InterpreterShowPrivilegesQuery(const ASTPtr & query_ptr_, Context & context_);

    BlockIO execute() override;

    bool ignoreQuota() const override { return false; }
    bool ignoreLimits() const override { return false; }

private:
    ASTPtr query_ptr;
    Context & context;
};

}
