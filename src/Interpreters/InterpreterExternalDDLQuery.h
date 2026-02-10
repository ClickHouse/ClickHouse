#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/IInterpreter.h>

namespace DB
{

class InterpreterExternalDDLQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterExternalDDLQuery(const ASTPtr & query_, ContextMutablePtr context_);

    BlockIO execute() override;

private:
    const ASTPtr query;

};

}
