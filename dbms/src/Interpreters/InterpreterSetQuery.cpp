#include <DB/Parsers/ASTSetQuery.h>
#include <DB/Interpreters/InterpreterSetQuery.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int READONLY;
}


BlockIO InterpreterSetQuery::execute()
{
	ASTSetQuery & ast = typeid_cast<ASTSetQuery &>(*query_ptr);
	Context & target = ast.global ? context.getGlobalContext() : context.getSessionContext();
	executeImpl(ast, target);
	return {};
}


void InterpreterSetQuery::executeForCurrentContext()
{
	ASTSetQuery & ast = typeid_cast<ASTSetQuery &>(*query_ptr);
	executeImpl(ast, context);
}


void InterpreterSetQuery::executeImpl(ASTSetQuery & ast, Context & target)
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


}
