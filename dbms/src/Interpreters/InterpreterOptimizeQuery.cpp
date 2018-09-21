#include <Storages/IStorage.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Common/typeid_cast.h>
#include <Common/Macros.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


BlockIO InterpreterOptimizeQuery::execute()
{
    ASTOptimizeQuery & ast = typeid_cast<ASTOptimizeQuery &>(*query_ptr);

    ast.database = context.getMacros()->expand(ast.database);
    ast.table = context.getMacros()->expand(ast.table);

    if (!ast.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, {ast.database});

    StoragePtr table = context.getTable(ast.database, ast.table);
    auto table_lock = table->lockStructure(true, __PRETTY_FUNCTION__);
    table->optimize(query_ptr, ast.partition, ast.final, ast.deduplicate, context);
    return {};
}

}
