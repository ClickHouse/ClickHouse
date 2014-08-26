#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Parsers/ASTIdentifier.h>

namespace DB
{

class InterpreterCheckQuery
{
public:
	InterpreterCheckQuery(ASTPtr query_ptr_, Context & context_);
	BlockInputStreamPtr execute();
	DB::Block getSampleBlock();

private:
	ASTPtr query_ptr;
	Context context;
	DB::Block result;
};

}
