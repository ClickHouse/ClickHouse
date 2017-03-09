#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>


namespace DB
{


/** Вернуть одну строку с одним столбцом statement типа String с текстом запроса, создающего указанную таблицу.
	*/
class InterpreterShowCreateQuery : public IInterpreter
{
public:
	InterpreterShowCreateQuery(ASTPtr query_ptr_, Context & context_)
		: query_ptr(query_ptr_), context(context_) {}

	BlockIO execute() override;

private:
	ASTPtr query_ptr;
	Context context;

	Block getSampleBlock();
	BlockInputStreamPtr executeImpl();
};


}
