#include <Storages/IStorage.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Interpreters/InterpreterOptimizeQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


BlockIO InterpreterOptimizeQuery::execute()
{
    const ASTOptimizeQuery & ast = typeid_cast<const ASTOptimizeQuery &>(*query_ptr);

    if (ast.final && ast.partition.empty())
        throw Exception("FINAL flag for OPTIMIZE query is meaningful only with specified PARTITION", ErrorCodes::BAD_ARGUMENTS);

    StoragePtr table = context.getTable(ast.database, ast.table);
    auto table_lock = table->lockStructure(true);
    table->optimize(ast.partition, ast.final, ast.deduplicate, context.getSettings());
    return {};
}

}
