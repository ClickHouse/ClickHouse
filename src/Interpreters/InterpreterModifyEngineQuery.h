#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context.h>


namespace DB
{

class AccessRightsElements;
class ASTModifyEngineQuery;

/** Allows to change table's engine.
  */
class InterpreterModifyEngineQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterModifyEngineQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    bool supportsTransactions() const override { return false; }

private:
    AccessRightsElements getRequiredAccess() const;

    ASTPtr query_ptr;

    const bool internal = false;
};

}
