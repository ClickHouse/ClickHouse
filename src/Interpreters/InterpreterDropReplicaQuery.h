#pragma once

#include <Interpreters/IInterpreter.h>

namespace DB
{

class InterpreterFactory;

class InterpreterDropReplicaQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterDropReplicaQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterDropReplicaQuery(InterpreterFactory & factory);

}
