#pragma once

#include <DB/Interpreters/IInterpreter.h>


namespace DB
{

class Context;


/** Выбрать БД по-умолчанию для сессии.
  */
class InterpreterUseQuery : public IInterpreter
{
public:
	InterpreterUseQuery(ASTPtr query_ptr_, Context & context_)
		: query_ptr(query_ptr_), context(context_) {}

	BlockIO execute() override;

private:
	ASTPtr query_ptr;
	Context & context;
};


}
