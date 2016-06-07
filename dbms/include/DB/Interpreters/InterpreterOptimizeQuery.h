#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Parsers/ASTOptimizeQuery.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>


namespace DB
{


/** Просто вызвать метод optimize у таблицы.
  */
class InterpreterOptimizeQuery : public IInterpreter
{
public:
	InterpreterOptimizeQuery(ASTPtr query_ptr_, Context & context_)
		: query_ptr(query_ptr_), context(context_)
	{
	}

	BlockIO execute() override
	{
		const ASTOptimizeQuery & ast = typeid_cast<const ASTOptimizeQuery &>(*query_ptr);
		StoragePtr table = context.getTable(ast.database, ast.table);
		auto table_lock = table->lockStructure(true);
		table->optimize(context.getSettings());
		return {};
	}

private:
	ASTPtr query_ptr;
	Context context;
};


}
