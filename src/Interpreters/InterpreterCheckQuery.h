#pragma once

#include <Core/Block.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class Cluster;

class InterpreterCheckQuery : public IInterpreter, WithContext
{
public:
    InterpreterCheckQuery(const ASTPtr & query_ptr_, ContextPtr context_);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;

    LoggerPtr log = getLogger("InterpreterCheckQuery");
};

}
