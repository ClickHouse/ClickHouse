#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterFactory;

/// Executes `CREATE REPLICA` by delegating to `CREATE NAMED COLLECTION` with the same properties.
class InterpreterCreateReplicaQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateReplicaQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterCreateReplicaQuery(InterpreterFactory & factory);

}
