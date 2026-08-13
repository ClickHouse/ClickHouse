#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterFactory;

/// Interpreter for both `CREATE CLUSTER` and `CREATE SHARD`
/// (`ASTCreateClusterCatalogQuery`).
class InterpreterCreateClusterCatalogQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateClusterCatalogQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterCreateClusterCatalogQuery(InterpreterFactory & factory);

}
