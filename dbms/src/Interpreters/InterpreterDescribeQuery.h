#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>


namespace DB
{


/** Вернуть названия и типы столбцов указанной таблицы.
    */
class InterpreterDescribeQuery : public IInterpreter
{
public:
    InterpreterDescribeQuery(ASTPtr query_ptr_, const Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context context;

    Block getSampleBlock();
    BlockInputStreamPtr executeImpl();
};


}
