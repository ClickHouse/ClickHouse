#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>


namespace DB
{


/** Проверить, существует ли таблица. Вернуть одну строку с одним столбцом result типа UInt8 со значением 0 или 1.
  */
class InterpreterExistsQuery : public IInterpreter
{
public:
    InterpreterExistsQuery(ASTPtr query_ptr_, Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context context;

    Block getSampleBlock();
    BlockInputStreamPtr executeImpl();
};


}
