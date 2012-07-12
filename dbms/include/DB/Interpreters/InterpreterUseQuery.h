#pragma once

#include <DB/Parsers/ASTUseQuery.h>
#include <DB/Interpreters/Context.h>


namespace DB
{


/** Выбрать БД по-умолчанию для сессии.
  */
class InterpreterUseQuery
{
public:
	InterpreterUseQuery(ASTPtr query_ptr_, Context & context_)
		: query_ptr(query_ptr_), context(context_) {}

	void execute()
	{
		const String & new_database = dynamic_cast<const ASTUseQuery &>(*query_ptr).database;

		{
			Poco::ScopedLock<Poco::Mutex> lock(*context.mutex);
			if (context.databases->end() == context.databases->find(new_database))
				throw Exception("Database " + new_database + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
		}

		if (!context.session_context)
			throw Exception("There is no session", ErrorCodes::THERE_IS_NO_SESSION);
			
		context.session_context->current_database = new_database;
	}

private:
	ASTPtr query_ptr;
	Context & context;
};


}
