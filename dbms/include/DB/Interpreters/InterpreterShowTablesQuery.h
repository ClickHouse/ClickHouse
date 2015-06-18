#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>


namespace DB
{


/** Вывести список имён таблиц/баз данных по некоторым условиям.
  * Интерпретирует запрос путём замены его на запрос SELECT из таблицы system.tables или system.databases.
  */
class InterpreterShowTablesQuery : public IInterpreter
{
public:
	InterpreterShowTablesQuery(ASTPtr query_ptr_, Context & context_);

	BlockIO execute() override;

private:
	ASTPtr query_ptr;
	Context context;

	String getRewrittenQuery();
};


}
