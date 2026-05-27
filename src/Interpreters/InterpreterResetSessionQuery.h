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

private:
    ASTPtr query_ptr;
};

}
