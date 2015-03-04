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

		/** Значение readonly понимается следующим образом:
		  * 0 - можно всё.
		  * 1 - можно делать только запросы на чтение; в том числе, нельзя менять настройки.
		  * 2 - можно делать только запросы на чтение и можно менять настройки, кроме настройки readonly.
		  */

		if (context.getSettingsRef().limits.readonly == 1)
			throw Exception("Cannot execute SET query in readonly mode", ErrorCodes::READONLY);

		if (context.getSettingsRef().limits.readonly > 1)
			for (ASTSetQuery::Changes::const_iterator it = ast.changes.begin(); it != ast.changes.end(); ++it)
				if (it->name == "readonly")
					throw Exception("Cannot modify 'readonly' setting in readonly mode", ErrorCodes::READONLY);

		for (ASTSetQuery::Changes::const_iterator it = ast.changes.begin(); it != ast.changes.end(); ++it)
			target.setSetting(it->name, it->value);
	}

private:
	ASTPtr query_ptr;
	Context & context;
};


}
