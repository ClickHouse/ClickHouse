#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterFactory;

class InterpreterCreateClusterQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateClusterQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterCreateClusterQuery(InterpreterFactory & factory);

}
