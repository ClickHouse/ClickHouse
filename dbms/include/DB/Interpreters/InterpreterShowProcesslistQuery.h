#pragma once

#include <DB/Interpreters/IInterpreter.h>
#include <DB/Interpreters/Context.h>


namespace DB
{


/** Вернуть список запросов, исполняющихся прямо сейчас.
  */
class InterpreterShowProcesslistQuery : public IInterpreter
{
public:
	InterpreterShowProcesslistQuery(ASTPtr query_ptr_, Context & context_)
		: query_ptr(query_ptr_), context(context_) {}

	BlockIO execute() override;

private:
	ASTPtr query_ptr;
	Context context;

	String getRewrittenQuery();
};


}
