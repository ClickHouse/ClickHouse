#include <Storages/IStorage.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Access/AccessRightsElement.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


BlockIO InterpreterOptimizeQuery::execute()
{
    const auto & ast = query_ptr->as<ASTOptimizeQuery &>();

    if (!ast.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, getRequiredAccess());

    context.checkAccess(getRequiredAccess());

    StoragePtr table = DatabaseCatalog::instance().getTable(StorageID{ast, context});
    table->optimize(query_ptr, ast.partition, ast.final, ast.deduplicate, context);
    return {};
}


AccessRightsElements InterpreterOptimizeQuery::getRequiredAccess() const
{
    const auto & optimize = query_ptr->as<const ASTOptimizeQuery &>();
    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::OPTIMIZE, optimize.database, optimize.table);
    return required_access;
}

}
