#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterFactory;

/// Executes `ALTER ENDPOINT` by committing endpoint metadata through `ClusterMetadataManager`.
class InterpreterAlterEndpointQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterAlterEndpointQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterAlterEndpointQuery(InterpreterFactory & factory);

}
