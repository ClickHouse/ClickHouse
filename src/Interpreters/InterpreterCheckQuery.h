#pragma once

#include <Core/Block.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

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
};

}
