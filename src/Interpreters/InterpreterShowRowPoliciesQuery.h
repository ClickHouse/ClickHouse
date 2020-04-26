#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;

class InterpreterShowRowPoliciesQuery : public IInterpreter
{
public:
    InterpreterShowRowPoliciesQuery(const ASTPtr & query_ptr_, Context & context_);
    BlockIO execute() override;

private:
    String getRewrittenQuery() const;
    String getResultDescription() const;

    ASTPtr query_ptr;
    Context & context;
};

}
