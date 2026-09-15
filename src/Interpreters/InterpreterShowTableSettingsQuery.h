#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class Context;

/// Returns a single row for each setting of one table, by rewriting into a `SELECT` over
/// `system.table_settings`.
class InterpreterShowTableSettingsQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterShowTableSettingsQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    ASTPtr query_ptr;

    String getRewrittenQuery();
};

}
