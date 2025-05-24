#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterCreateModelQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateModelQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
