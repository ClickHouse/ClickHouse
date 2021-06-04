#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/IInterpreter.h>

namespace DB
{

class InterpreterExternalDDLQuery : public IInterpreter, WithContext
{
public:
    InterpreterExternalDDLQuery(const ASTPtr & query_, ContextPtr context_);

    BlockIO execute() override;

private:
    const ASTPtr query;

};

}
