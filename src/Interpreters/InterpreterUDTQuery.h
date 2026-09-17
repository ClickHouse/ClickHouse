#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Database-scoped UDT lifecycle and explicit physicalization dispatcher.
/// Durable behavior remains behind the database's fail-closed adapter.
class InterpreterUDTQuery final : public IInterpreter, WithMutableContext
{
public:
    InterpreterUDTQuery(ASTPtr query_, ContextMutablePtr context_);
    BlockIO execute() override;

private:
    ASTPtr query;
};

}
