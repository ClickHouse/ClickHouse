#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Parsers/ASTOptimizeQuery.h>
#include <DB/Interpreters/Context.h>


namespace DB
{


/** Просто вызвать метод optimize у таблицы.
  */
class InterpreterOptimizeQuery
{
public:
	InterpreterOptimizeQuery(ASTPtr query_ptr_, Context & context_)
		: query_ptr(query_ptr_), context(context_) {}

	void execute()
	{
		Poco::ScopedLock<Poco::Mutex> lock(*context.mutex);

		const ASTOptimizeQuery & ast = dynamic_cast<const ASTOptimizeQuery &>(*query_ptr);
		context.assertTableExists(ast.database, ast.table);

		(*context.databases)[ast.database.empty() ? context.current_database : ast.database][ast.table]->optimize();
	}

private:
	ASTPtr query_ptr;
	Context context;
};


}
