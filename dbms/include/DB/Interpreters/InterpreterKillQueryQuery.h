#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>


namespace DB
{


class InterpreterKillQueryQuery : public IInterpreter
{
public:
	InterpreterKillQueryQuery(ASTPtr query_ptr_, Context & context_)
		: query_ptr(query_ptr_), context(context_) {}

	BlockIO execute() override;

private:

	Block getSelectFromSystemProcessesResult();

	ASTPtr query_ptr;
	Context context;
};


}

