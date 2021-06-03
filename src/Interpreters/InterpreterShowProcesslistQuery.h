#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;


/** Return list of currently executing queries.
  */
class InterpreterShowProcesslistQuery : public IInterpreter
{
public:
    InterpreterShowProcesslistQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    /// We ignore the quota and limits here because execute() will rewrite a show query as a SELECT query and then
    /// the SELECT query will checks the quota and limits.
    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    ASTPtr query_ptr;
    Context & context;
};


}
