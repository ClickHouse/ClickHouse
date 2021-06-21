#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/IInterpreter.h>

namespace DB
{

class InterpreterExternalDDLQuery : public IInterpreter
{
public:
    InterpreterExternalDDLQuery(const ASTPtr & query_, Context & context_);

    BlockIO execute() override;
private:
    const ASTPtr query;
    Context & context;

};

}
