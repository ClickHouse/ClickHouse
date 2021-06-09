#pragma once

#include <Interpreters/IInterpreter.h>

namespace DB
{

class ASTDropFunctionQuery;

class InterpreterDropFunctionQuery : public IInterpreter, WithContext
{
public:
    InterpreterDropFunctionQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
