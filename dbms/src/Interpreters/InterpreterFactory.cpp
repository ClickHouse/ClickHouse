#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTDropQuery.h>
#include <DB/Parsers/ASTRenameQuery.h>
#include <DB/Parsers/ASTShowTablesQuery.h>
#include <DB/Parsers/ASTUseQuery.h>
#include <DB/Parsers/ASTSetQuery.h>
#include <DB/Parsers/ASTOptimizeQuery.h>
#include <DB/Parsers/ASTAlterQuery.h>
#include <DB/Parsers/ASTShowProcesslistQuery.h>
#include <DB/Parsers/TablePropertiesQueriesASTs.h>
#include <DB/Parsers/ASTCheckQuery.h>

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
#include <DB/Interpreters/InterpreterDescribeQuery.h>
#include <DB/Interpreters/InterpreterShowCreateQuery.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Interpreters/InterpreterShowProcesslistQuery.h>
#include <DB/Interpreters/InterpreterCheckQuery.h>
#include <DB/Interpreters/InterpreterFactory.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int READONLY;
	extern const int UNKNOWN_TYPE_OF_QUERY;
}


static void throwIfReadOnly(Context & context)
{
	if (context.getSettingsRef().limits.readonly)
		throw Exception("Cannot execute query in readonly mode", ErrorCodes::READONLY);
}


std::unique_ptr<IInterpreter> InterpreterFactory::get(ASTPtr & query, Context & context, QueryProcessingStage::Enum stage)
{
	if (typeid_cast<ASTSelectQuery *>(query.get()))
	{
		return std::make_unique<InterpreterSelectQuery>(query, context, stage);
	}
	else if (typeid_cast<ASTInsertQuery *>(query.get()))
	{
		throwIfReadOnly(context);
		return std::make_unique<InterpreterInsertQuery>(query, context);
	}
	else if (typeid_cast<ASTCreateQuery *>(query.get()))
	{
		throwIfReadOnly(context);
		return std::make_unique<InterpreterCreateQuery>(query, context);
	}
	else if (typeid_cast<ASTDropQuery *>(query.get()))
	{
		throwIfReadOnly(context);
		return std::make_unique<InterpreterDropQuery>(query, context);
	}
	else if (typeid_cast<ASTRenameQuery *>(query.get()))
	{
		throwIfReadOnly(context);
		return std::make_unique<InterpreterRenameQuery>(query, context);
	}
	else if (typeid_cast<ASTShowTablesQuery *>(query.get()))
	{
		return std::make_unique<InterpreterShowTablesQuery>(query, context);
	}
	else if (typeid_cast<ASTUseQuery *>(query.get()))
	{
		return std::make_unique<InterpreterUseQuery>(query, context);
	}
	else if (typeid_cast<ASTSetQuery *>(query.get()))
	{
		/// readonly проверяется внутри InterpreterSetQuery
		return std::make_unique<InterpreterSetQuery>(query, context);
	}
	else if (typeid_cast<ASTOptimizeQuery *>(query.get()))
	{
		throwIfReadOnly(context);
		return std::make_unique<InterpreterOptimizeQuery>(query, context);
	}
	else if (typeid_cast<ASTExistsQuery *>(query.get()))
	{
		return std::make_unique<InterpreterExistsQuery>(query, context);
	}
	else if (typeid_cast<ASTShowCreateQuery *>(query.get()))
	{
		return std::make_unique<InterpreterShowCreateQuery>(query, context);
	}
	else if (typeid_cast<ASTDescribeQuery *>(query.get()))
	{
		return std::make_unique<InterpreterDescribeQuery>(query, context);
	}
	else if (typeid_cast<ASTShowProcesslistQuery *>(query.get()))
	{
		return std::make_unique<InterpreterShowProcesslistQuery>(query, context);
	}
	else if (typeid_cast<ASTAlterQuery *>(query.get()))
	{
		throwIfReadOnly(context);
		return std::make_unique<InterpreterAlterQuery>(query, context);
	}
	else if (typeid_cast<ASTCheckQuery *>(query.get()))
	{
		return std::make_unique<InterpreterCheckQuery>(query, context);
	}
	else
		throw Exception("Unknown type of query: " + query->getID(), ErrorCodes::UNKNOWN_TYPE_OF_QUERY);
}

}
