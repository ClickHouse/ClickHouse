#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Interpreters/InterpreterFactory.h>

namespace DB
{

class Context;

class InterpreterCreateTypeQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateTypeQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterCreateTypeQuery(InterpreterFactory & factory);

}
