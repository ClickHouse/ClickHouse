#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterFactory;

/// Executes `CREATE ENDPOINT` by committing endpoint metadata through `ClusterMetadataManager`.
class InterpreterCreateEndpointQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateEndpointQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterCreateEndpointQuery(InterpreterFactory & factory);

}
