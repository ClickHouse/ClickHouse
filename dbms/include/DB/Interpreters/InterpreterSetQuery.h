#pragma once

#include <DB/Parsers/ASTSetQuery.h>
#include <DB/Interpreters/Context.h>


namespace DB
{


/** Установить один или несколько параметров, для сессии или глобально.
  */
class InterpreterSetQuery
{
public:
	InterpreterSetQuery(ASTPtr query_ptr_, Context & context_)
		: query_ptr(query_ptr_), context(context_) {}

	void execute()
	{
		ASTSetQuery & ast = typeid_cast<ASTSetQuery &>(*query_ptr);

		Context & target = ast.global ? context.getGlobalContext() : context.getSessionContext();

		for (ASTSetQuery::Changes::const_iterator it = ast.changes.begin(); it != ast.changes.end(); ++it)
			target.setSetting(it->name, it->value);
	}

private:
	ASTPtr query_ptr;
	Context & context;
};


}
