#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/InterpreterUndropQuery.h>
#include <Access/Common/AccessRightsElement.h>
#include <Parsers/ASTUndropQuery.h>

#include "config.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int SUPPORT_IS_DISABLED;
}

InterpreterUndropQuery::InterpreterUndropQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterUndropQuery::execute()
{
    if (!getContext()->getSettingsRef().allow_experimental_undrop_table_query)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "Undrop table is experimental. "
                        "Set `allow_experimental_undrop_table_query` setting to enable it");

    getContext()->checkAccess(AccessType::UNDROP_TABLE);
    auto & undrop = query_ptr->as<ASTUndropQuery &>();
    if (!undrop.cluster.empty() && !maybeRemoveOnCluster(query_ptr, getContext()))
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccessForDDLOnCluster();
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    if (undrop.table)
        return executeToTable(undrop);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Nothing to undrop, both names are empty");
}

BlockIO InterpreterUndropQuery::executeToTable(ASTUndropQuery & query)
{
    auto table_id = StorageID(query);

    auto context = getContext();
    if (table_id.database_name.empty())
    {
        table_id.database_name = context->getCurrentDatabase();
        query.setDatabase(table_id.database_name);
    }

    auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name);

    auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (database->getEngineName() == "Replicated")
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Replicated database does not support UNDROP query");
    if (database->isTableExist(table_id.table_name, getContext()))
        throw Exception(
            ErrorCodes::TABLE_ALREADY_EXISTS, "Cannot undrop table, {} already exists", table_id);

    database->checkMetadataFilenameAvailability(table_id.table_name);

    DatabaseCatalog::instance().dequeueDroppedTableCleanup(table_id);
    return {};
}

AccessRightsElements InterpreterUndropQuery::getRequiredAccessForDDLOnCluster() const
{
    AccessRightsElements required_access;
    const auto & undrop = query_ptr->as<const ASTUndropQuery &>();

    required_access.emplace_back(AccessType::UNDROP_TABLE, undrop.getDatabase(), undrop.getTable());
    return required_access;
}
}
