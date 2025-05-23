#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>

namespace DB
{

class Context;

/** Interprets the SHOW TYPE type_name query.
  */
class InterpreterShowTypeQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterShowTypeQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

void registerInterpreterShowTypeQuery(InterpreterFactory & factory);

}
