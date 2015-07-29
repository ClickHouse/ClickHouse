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
		const String & new_database = typeid_cast<const ASTUseQuery &>(*query_ptr).database;
		context.getSessionContext().setCurrentDatabase(new_database);
	}

private:
	ASTPtr query_ptr;
	Context & context;
};


}
