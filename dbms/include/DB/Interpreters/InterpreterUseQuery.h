#pragma once

#include <DB/Parsers/ASTUseQuery.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>


namespace DB
{


/** Выбрать БД по-умолчанию для сессии.
  */
class InterpreterUseQuery : public IInterpreter
{
public:
	InterpreterUseQuery(ASTPtr query_ptr_, Context & context_)
		: query_ptr(query_ptr_), context(context_) {}

	BlockIO execute() override
	{
		const String & new_database = typeid_cast<const ASTUseQuery &>(*query_ptr).database;
		context.getSessionContext().setCurrentDatabase(new_database);
		return {};
	}

private:
	ASTPtr query_ptr;
	Context & context;
};


}
