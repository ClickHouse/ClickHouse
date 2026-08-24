#pragma once

#include <Interpreters/IInterpreter.h>

namespace DB
{

class Context;

class InterpreterDropFunctionQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterDropFunctionQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
