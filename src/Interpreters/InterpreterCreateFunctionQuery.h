#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTCreateFunctionQuery.h>

namespace DB
{

class ASTCreateFunctionQuery;

class InterpreterCreateFunctionQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateFunctionQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
