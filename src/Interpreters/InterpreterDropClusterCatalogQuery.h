#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterFactory;

/// Interpreter for both `DROP CLUSTER` and `DROP SHARD` (`ASTDropClusterCatalogQuery`).
class InterpreterDropClusterCatalogQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterDropClusterCatalogQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterDropClusterCatalogQuery(InterpreterFactory & factory);

}
