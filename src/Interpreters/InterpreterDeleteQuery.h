#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTDeleteQuery.h>

namespace DB
{
class InterpreterDeleteQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterDeleteQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};
}
