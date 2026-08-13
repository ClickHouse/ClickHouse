#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterFactory;

/// Interpreter for both `SHOW CREATE CLUSTER` and `SHOW CREATE SHARD`
/// (`ASTShowCreateClusterCatalogQuery`).
class InterpreterShowCreateClusterCatalogQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterShowCreateClusterCatalogQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterShowCreateClusterCatalogQuery(InterpreterFactory & factory);

}
