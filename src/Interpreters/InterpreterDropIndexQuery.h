#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;

class InterpreterDropIndexQuery : public IInterpreter, WithContext
{
public:
    InterpreterDropIndexQuery(const ASTPtr & query_ptr_, ContextPtr context_)
        : WithContext(context_)
        , query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
