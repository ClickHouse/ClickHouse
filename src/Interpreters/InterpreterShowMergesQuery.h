#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/** Return list of merges and part mutations currently in process for tables in the MergeTree family
  */
class InterpreterShowMergesQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterShowMergesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    /// We ignore the quota and limits here because execute() will rewrite a show query as a SELECT query and then
    /// the SELECT query will checks the quota and limits.
    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    ASTPtr query_ptr;
};

}
