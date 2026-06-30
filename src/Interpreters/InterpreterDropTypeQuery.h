#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTDropTypeQuery.h>
#include <Interpreters/Context.h>

namespace DB
{

class InterpreterDropTypeQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterDropTypeQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
