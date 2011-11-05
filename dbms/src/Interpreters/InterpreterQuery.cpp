#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTDropQuery.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterInsertQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/InterpreterDropQuery.h>
#include <DB/Interpreters/InterpreterQuery.h>


namespace DB
{


InterpreterQuery::InterpreterQuery(ASTPtr query_ptr_, Context & context_, size_t max_block_size_)
	: query_ptr(query_ptr_), context(context_), max_block_size(max_block_size_)
{
}


void InterpreterQuery::execute(WriteBuffer & ostr, SharedPtr<ReadBuffer> remaining_data_istr, BlockInputStreamPtr & query_plan)
{
	if (dynamic_cast<ASTSelectQuery *>(&*query_ptr))
	{
		InterpreterSelectQuery interpreter(query_ptr, context, max_block_size);
		query_plan = interpreter.executeAndFormat(ostr);
	}
	else if (dynamic_cast<ASTInsertQuery *>(&*query_ptr))
	{
		InterpreterInsertQuery interpreter(query_ptr, context, max_block_size);
		interpreter.execute(remaining_data_istr);
	}
	else if (dynamic_cast<ASTCreateQuery *>(&*query_ptr))
	{
		InterpreterCreateQuery interpreter(query_ptr, context, max_block_size);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTDropQuery *>(&*query_ptr))
	{
		InterpreterDropQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else
		throw Exception("Unknown type of query: " + query_ptr->getID(), ErrorCodes::UNKNOWN_TYPE_OF_QUERY);
}


}
