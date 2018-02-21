#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST.h>

namespace DB
{

class Context;
class Cluster;

class InterpreterCheckQuery : public IInterpreter
{
public:
    InterpreterCheckQuery(const ASTPtr & query_ptr_, const Context & context_);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;

    const Context & context;
    Block result;
};

}
