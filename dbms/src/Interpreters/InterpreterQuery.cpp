#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTDropQuery.h>
#include <DB/Parsers/ASTRenameQuery.h>
#include <DB/Parsers/ASTShowTablesQuery.h>
#include <DB/Parsers/ASTUseQuery.h>
#include <DB/Parsers/ASTSetQuery.h>
#include <DB/Parsers/ASTOptimizeQuery.h>
#include <DB/Parsers/ASTExistsQuery.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterInsertQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/InterpreterDropQuery.h>
#include <DB/Interpreters/InterpreterRenameQuery.h>
#include <DB/Interpreters/InterpreterShowTablesQuery.h>
#include <DB/Interpreters/InterpreterUseQuery.h>
#include <DB/Interpreters/InterpreterSetQuery.h>
#include <DB/Interpreters/InterpreterOptimizeQuery.h>
#include <DB/Interpreters/InterpreterExistsQuery.h>
#include <DB/Interpreters/InterpreterQuery.h>


namespace DB
{


InterpreterQuery::InterpreterQuery(ASTPtr query_ptr_, Context & context_, QueryProcessingStage::Enum stage_)
	: query_ptr(query_ptr_), context(context_), stage(stage_)
{
}


void InterpreterQuery::execute(WriteBuffer & ostr, ReadBuffer * remaining_data_istr, BlockInputStreamPtr & query_plan)
{
	if (dynamic_cast<ASTSelectQuery *>(&*query_ptr))
	{
		InterpreterSelectQuery interpreter(query_ptr, context, stage);
		query_plan = interpreter.executeAndFormat(ostr);
	}
	else if (dynamic_cast<ASTInsertQuery *>(&*query_ptr))
	{
		throwIfReadOnly();
		InterpreterInsertQuery interpreter(query_ptr, context);
		interpreter.execute(remaining_data_istr);
	}
	else if (dynamic_cast<ASTCreateQuery *>(&*query_ptr))
	{
		throwIfReadOnly();
		InterpreterCreateQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTDropQuery *>(&*query_ptr))
	{
		throwIfReadOnly();
		InterpreterDropQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTRenameQuery *>(&*query_ptr))
	{
		throwIfReadOnly();
		InterpreterRenameQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTShowTablesQuery *>(&*query_ptr))
	{
		InterpreterShowTablesQuery interpreter(query_ptr, context);
		query_plan = interpreter.executeAndFormat(ostr);
	}
	else if (dynamic_cast<ASTUseQuery *>(&*query_ptr))
	{
		InterpreterUseQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTSetQuery *>(&*query_ptr))
	{
		throwIfReadOnly();
		InterpreterSetQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTOptimizeQuery *>(&*query_ptr))
	{
		throwIfReadOnly();
		InterpreterOptimizeQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTExistsQuery *>(&*query_ptr))
	{
		InterpreterExistsQuery interpreter(query_ptr, context);
		query_plan = interpreter.executeAndFormat(ostr);
	}
	else
		throw Exception("Unknown type of query: " + query_ptr->getID(), ErrorCodes::UNKNOWN_TYPE_OF_QUERY);
}


BlockIO InterpreterQuery::execute()
{
	BlockIO res;
	
	if (dynamic_cast<ASTSelectQuery *>(&*query_ptr))
	{
		InterpreterSelectQuery interpreter(query_ptr, context, stage);
		res.in = interpreter.execute();
		res.in_sample = interpreter.getSampleBlock();
	}
	else if (dynamic_cast<ASTInsertQuery *>(&*query_ptr))
	{
		throwIfReadOnly();
		InterpreterInsertQuery interpreter(query_ptr, context);
		res.out = interpreter.execute();
		res.out_sample = interpreter.getSampleBlock();
	}
	else if (dynamic_cast<ASTCreateQuery *>(&*query_ptr))
	{
		throwIfReadOnly();
		InterpreterCreateQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTDropQuery *>(&*query_ptr))
	{
		throwIfReadOnly();
		InterpreterDropQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTRenameQuery *>(&*query_ptr))
	{
		throwIfReadOnly();
		InterpreterRenameQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTShowTablesQuery *>(&*query_ptr))
	{
		InterpreterShowTablesQuery interpreter(query_ptr, context);
		res = interpreter.execute();
	}
	else if (dynamic_cast<ASTUseQuery *>(&*query_ptr))
	{
		InterpreterUseQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTSetQuery *>(&*query_ptr))
	{
		throwIfReadOnly();
		InterpreterSetQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTOptimizeQuery *>(&*query_ptr))
	{
		throwIfReadOnly();
		InterpreterOptimizeQuery interpreter(query_ptr, context);
		interpreter.execute();
	}
	else if (dynamic_cast<ASTExistsQuery *>(&*query_ptr))
	{
		InterpreterExistsQuery interpreter(query_ptr, context);
		res = interpreter.execute();
	}
	else
		throw Exception("Unknown type of query: " + query_ptr->getID(), ErrorCodes::UNKNOWN_TYPE_OF_QUERY);

	return res;
}


}
