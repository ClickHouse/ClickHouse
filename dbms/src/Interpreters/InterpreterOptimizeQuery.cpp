#include <Storages/IStorage.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


BlockIO InterpreterOptimizeQuery::execute()
{
    const ASTOptimizeQuery & ast = typeid_cast<const ASTOptimizeQuery &>(*query_ptr);

    if (!ast.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, {ast.database});

    StoragePtr table = context.getTable(ast.database, ast.table);
    auto table_lock = table->lockStructure(true, __PRETTY_FUNCTION__);
    table->optimize(query_ptr, ast.partition, ast.final, ast.deduplicate, context);
    return {};
}

}
