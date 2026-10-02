#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterUndropQuery.h>
#include <Interpreters/ProcessList.h>
#include <Access/Common/AccessRightsElement.h>
#include <Parsers/ASTUndropQuery.h>
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

    auto & undrop = query_ptr->as<ASTUndropQuery &>();

    /// A hierarchical name (`UNDROP TABLE a.b.c`, or `c` inside `USE a.b`) is bound to the database and the table it denotes
    /// before the query is dispatched `ON CLUSTER` and executed (see `DatabaseCatalog`).
    if (undrop.table)
        resolveHierarchicalName(undrop);

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

void InterpreterUndropQuery::resolveHierarchicalName(ASTUndropQuery & query) const
{
    auto & catalog = DatabaseCatalog::instance();
    StorageID as_written(query.database ? query.getDatabase() : "", query.getTable());
    String current_database = getContext()->getCurrentDatabase();

    /// The table does not exist, so the name denotes the first candidate that has a dropped table to restore (with the
    /// given UUID, if any). Without such a candidate, it is the placement (the first candidate whose database exists), for
    /// the error message; a database that does not exist here keeps the name as written: `ON CLUSTER`, it may exist on the
    /// other hosts only.
    std::optional<StorageID> resolved;
    if (as_written.database_name.contains('.') || as_written.table_name.contains('.') || current_database.contains('.'))
    {
        auto dropped_tables = catalog.getTablesMarkedDropped();
        for (const auto & candidate : DatabaseCatalog::getHierarchicalNameCandidates(as_written, current_database))
        {
            bool has_dropped_table = std::any_of(dropped_tables.begin(), dropped_tables.end(), [&](const auto & dropped_table)
            {
                return dropped_table.table_id.database_name == candidate.database_name
                    && dropped_table.table_id.table_name == candidate.table_name
                    && (query.uuid == UUIDHelpers::Nil || dropped_table.table_id.uuid == query.uuid);
            });
            if (has_dropped_table)
            {
                resolved = candidate;
                break;
            }
        }
    }
    if (!resolved)
        resolved = catalog.getHierarchicalNamePlacement(as_written, current_database, getContext());

    if (resolved->database_name != (query.database ? query.getDatabase() : current_database) || resolved->table_name != as_written.table_name)
    {
        query.setDatabase(resolved->database_name);
        query.setTable(resolved->table_name);
    }
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
    if (SharedDatabaseCatalog::shouldReplicateQuery(getContext(), query_ptr))
    {
        SharedDatabaseCatalog::instance().undropTable(database->getUUID(), table_id.table_name);
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
