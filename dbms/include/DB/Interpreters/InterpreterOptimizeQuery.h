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
		: query_ptr(query_ptr_), context(context_),
		aio_threshold(context_.getSettings().min_bytes_to_use_direct_io) {}

	void execute()
	{
		const ASTOptimizeQuery & ast = typeid_cast<const ASTOptimizeQuery &>(*query_ptr);
		StoragePtr table = context.getTable(ast.database, ast.table);
		auto table_lock = table->lockStructure(true);
		table->optimize(aio_threshold);
	}

private:
	ASTPtr query_ptr;
	Context context;
	size_t aio_threshold;
};


}
