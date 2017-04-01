#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>


namespace DB
{

class ASTSetQuery;

/** Установить один или несколько параметров, для сессии или глобально... или для текущего запроса.
  */
class InterpreterSetQuery : public IInterpreter
{
public:
    InterpreterSetQuery(ASTPtr query_ptr_, Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    /** Обычный запрос SET. Задать настройку на сессию или глобальную (если указано GLOBAL).
      */
    BlockIO execute() override;

    /** Задать настроку для текущего контекста (контекста запроса).
      * Используется для интерпретации секции SETTINGS в запросе SELECT.
      */
    void executeForCurrentContext();

private:
    ASTPtr query_ptr;
    Context & context;

    void executeImpl(ASTSetQuery & ast, Context & target);
};


}
