#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterFactory;

/// Executes `DROP ENDPOINT` by committing endpoint metadata through `ClusterMetadataManager`.
class InterpreterDropEndpointQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterDropEndpointQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterDropEndpointQuery(InterpreterFactory & factory);

}
