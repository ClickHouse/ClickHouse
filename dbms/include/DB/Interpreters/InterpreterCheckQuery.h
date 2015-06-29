#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>
#include <DB/Parsers/ASTIdentifier.h>

namespace DB
{

class InterpreterCheckQuery : public IInterpreter
{
public:
	InterpreterCheckQuery(ASTPtr query_ptr_, Context & context_);
	BlockIO execute() override;

private:
	ASTPtr query_ptr;
	Context context;
	Block result;
};

}
