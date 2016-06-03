#pragma once

#include <DB/Parsers/ASTSetQuery.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int READONLY;
}


/** Установить один или несколько параметров, для сессии или глобально... или для текущего запроса.
  */
class InterpreterSetQuery : public IInterpreter
{
public:
	InterpreterSetQuery(ASTPtr query_ptr_, Context & context_)
		: query_ptr(query_ptr_), context(context_) {}

	/** Обычный запрос SET. Задать настройку на сессию или глобальную (если указано GLOBAL).
	  */
	BlockIO execute() override
	{
		ASTSetQuery & ast = typeid_cast<ASTSetQuery &>(*query_ptr);
		Context & target = ast.global ? context.getGlobalContext() : context.getSessionContext();
		executeImpl(ast, target);
		return {};
	}

	/** Задать настроку для текущего контекста (контекста запроса).
	  * Используется для интерпретации секции SETTINGS в запросе SELECT.
	  */
	void executeForCurrentContext()
	{
		ASTSetQuery & ast = typeid_cast<ASTSetQuery &>(*query_ptr);
		executeImpl(ast, context);
	}

private:
	ASTPtr query_ptr;
	Context & context;

	void executeImpl(ASTSetQuery & ast, Context & target)
	{
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
};


}
