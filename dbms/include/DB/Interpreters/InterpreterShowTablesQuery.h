#pragma once

#include <DB/DataStreams/BlockIO.h>

#include <DB/Interpreters/Context.h>


namespace DB
{


/** Вывести список имён таблиц/баз данных по некоторым условиям.
  * Интерпретирует запрос путём замены его на запрос SELECT из таблицы system.tables или system.databases.
  */
class InterpreterShowTablesQuery
{
public:
	InterpreterShowTablesQuery(ASTPtr query_ptr_, Context & context_);

	BlockIO execute();
	BlockInputStreamPtr executeAndFormat(WriteBuffer & buf);

private:
	ASTPtr query_ptr;
	Context context;

	String getRewrittenQuery();
};


}
