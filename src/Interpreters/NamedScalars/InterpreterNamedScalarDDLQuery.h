#pragma once

#include <Interpreters/IInterpreter.h>

namespace DB
{

class InterpreterNamedScalarDDLQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterNamedScalarDDLQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
