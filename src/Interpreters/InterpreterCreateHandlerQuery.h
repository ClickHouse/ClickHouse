#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

/// Handles both CREATE HANDLER and ALTER HANDLER (the AST carries the is_alter flag).
class InterpreterCreateHandlerQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateHandlerQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
