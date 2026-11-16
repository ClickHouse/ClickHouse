#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterUndropQuery.h>
#include <Interpreters/ProcessList.h>
#include <Access/Common/AccessRightsElement.h>
#include <Parsers/ASTUndropQuery.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#if CLICKHOUSE_CLOUD
#include <Interpreters/SharedDatabaseCatalog.h>
#endif

#include "config.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int SUPPORT_IS_DISABLED;
}

InterpreterUndropQuery::InterpreterUndropQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_)
    , query_ptr(query_ptr_)
{
}

BlockIO InterpreterUndropQuery::execute()
{
    getContext()->checkAccess(AccessType::UNDROP_TABLE);

    auto component_guard = Coordination::setCurrentComponent("InterpreterUndropQuery::execute");
    auto & undrop = query_ptr->as<ASTUndropQuery &>();
    if (!undrop.cluster.empty() && !maybeRemoveOnCluster(query_ptr, getContext()))
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccessForDDLOnCluster();
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    if (undrop.table)
        return executeToTable(undrop);
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

    auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name, nullptr);

    auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (database->getEngineName() == "Replicated")
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Replicated database does not support UNDROP query");
    if (database->isTableExist(table_id.table_name, getContext()))
        throw Exception(
            ErrorCodes::TABLE_ALREADY_EXISTS, "Cannot undrop table, {} already exists", table_id);

    database->checkMetadataFilenameAvailability(table_id.table_name);

#if CLICKHOUSE_CLOUD
    if (database->getEngineName() == "Shared")
    {
        if (!getContext()->getClientInfo().is_shared_catalog_internal)
        {
            chassert(guard);
            guard.reset();
            return SharedDatabaseCatalog::instance().tryExecuteDDLQuery(query_ptr, getContext());
        }

        auto version = SharedDatabaseCatalog::instance().undropTable(database->getUUID(), table_id.table_name, table_id.uuid);
        getContext()->setVersionToWaitSharedCatalog(version);
        return {};
    }
#endif

    QueryStatusPtr query_status = context->getProcessListElementSafe();
    auto throw_if_cancelled = [&]()
    {
        if (query_status)
            query_status->throwIfKilled();
    };

    DatabaseCatalog::instance().undropTable(table_id, throw_if_cancelled);
    return {};
}

AccessRightsElements InterpreterUndropQuery::getRequiredAccessForDDLOnCluster() const
{
    AccessRightsElements required_access;
    const auto & undrop = query_ptr->as<const ASTUndropQuery &>();

    required_access.emplace_back(AccessType::UNDROP_TABLE, undrop.getDatabase(), undrop.getTable());
    return required_access;
}

void registerInterpreterUndropQuery(InterpreterFactory & factory);
void registerInterpreterUndropQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterUndropQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterUndropQuery", create_fn);
}
}
