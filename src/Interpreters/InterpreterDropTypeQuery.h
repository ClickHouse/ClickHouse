#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTDropTypeQuery.h> // Используем наш новый AST
#include <Interpreters/Context.h>     // Для WithMutableContext

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

// Объявление функции регистрации, если мы будем выносить ее отдельно
// void registerInterpreterDropTypeQuery(InterpreterFactory & factory);

}
