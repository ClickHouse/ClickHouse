#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>

namespace DB
{

class Context;

/** Interprets the SHOW TYPES query.
  */
class InterpreterShowTypesQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterShowTypesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterShowTypesQuery(InterpreterFactory & factory);

}
