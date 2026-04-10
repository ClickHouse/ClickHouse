#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterFactory;

class InterpreterCreateShardQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateShardQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterCreateShardQuery(InterpreterFactory & factory);

}
