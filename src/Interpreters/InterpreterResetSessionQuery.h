#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/** Restore the current session to its post-authentication state.
  * See ASTResetSessionQuery for the semantics.
  */
class InterpreterResetSessionQuery : public IInterpreter, WithContext
{
public:
    InterpreterResetSessionQuery(const ASTPtr & query_ptr_, ContextPtr context_)
        : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    /// Reset itself doesn't run any query work — opt out of the
    /// `throw_on_unsupported_query_inside_transaction` gate so that
    /// `executeQuery` lets us through to `Context::resetToUserDefaults`,
    /// which is where the actual in-transaction rejection lives.
    bool supportsTransactions() const override { return true; }

private:
    ASTPtr query_ptr;
};

}
