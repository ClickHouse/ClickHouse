#include <DB/Storages/IStorage.h>
#include <DB/Parsers/ASTOptimizeQuery.h>
#include <DB/Interpreters/InterpreterOptimizeQuery.h>


namespace DB
{

BlockIO InterpreterOptimizeQuery::execute()
{
	const ASTOptimizeQuery & ast = typeid_cast<const ASTOptimizeQuery &>(*query_ptr);

	if (ast.final && ast.partition.empty())
		throw Exception("FINAL flag for OPTIMIZE query is meaningful only with specified PARTITION", ErrorCodes::BAD_ARGUMENTS);

	StoragePtr table = context.getTable(ast.database, ast.table);
	auto table_lock = table->lockStructure(true);
	table->optimize(ast.partition, ast.final, context.getSettings());
	return {};
}

}
