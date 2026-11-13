#include <Interpreters/reattachRandomTables.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/thread_local_rng.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageProxy.h>

#include <random>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int DEADLOCK_AVOIDED;
    extern const int TABLE_IS_DROPPED;
}

namespace Setting
{
    extern const SettingsFloat reattach_tables_before_query_execution_probability;
    extern const SettingsBool implicit_transaction;
}

namespace
{

/// The storage that the operations on `table` are forwarded to: the one behind a lazily loaded table,
/// the target of a materialized view or an alias. Null if there is none, or it is not loaded.
StoragePtr getForwardedStorage(const StoragePtr & table)
{
    if (const auto * proxy = dynamic_cast<const StorageProxy *>(table.get()))
        return proxy->tryGetNested();
    if (const auto * materialized_view = dynamic_cast<const StorageMaterializedView *>(table.get()))
        return materialized_view->tryGetTargetTable();
    if (const auto * alias = dynamic_cast<const StorageAlias *>(table.get()))
        return alias->tryGetTargetTable();
    return nullptr;
}

/// The action locks (e.g. from `SYSTEM STOP MERGES`) belong to the storage object and are lost on reattach.
/// `ActionLocksManager` keeps a lock under the storage it was requested for, but the lock may block the actions
/// of another storage: `SYSTEM STOP MERGES mv` stops merges of the inner table of `mv`.
/// So collect the storages with action locks together with all storages they forward to.
/// The owner of a lock may be in another database (e.g. an `Alias` of a table of the current database),
/// so look at the tables of all local databases.
std::unordered_set<const IStorage *> getStoragesWithActionLocks(const ContextPtr & context)
{
    std::unordered_set<const IStorage *> result;
    const auto action_locks_manager = context->getActionLocksManager();

    /// Usually there are no action locks at all, then there is no need to look at all tables.
    if (!action_locks_manager->hasAny())
        return result;

    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false, .with_remote_databases = false});
    for (const auto & [_, database] : databases)
    {
        for (auto it = database->getTablesIterator(context, {}, /* skip_not_loaded= */ true); it->isValid(); it->next())
        {
            const auto & table = it->table();
            if (!table || !action_locks_manager->hasAny(table))
                continue;

            /// The set also protects from cycles of aliases.
            for (StoragePtr storage = table; storage && result.insert(storage.get()).second; storage = getForwardedStorage(storage))
            {
                /// The catalog may hold the target as a lazily loaded table.
                if (auto * proxy = dynamic_cast<StorageProxy *>(storage.get()))
                    if (auto nested = proxy->tryGetNested())
                        result.insert(nested.get());
            }
        }
    }

    return result;
}

/// Reattaching these tables would change what the queries see, which is not a bug.
bool canBeReattached(const StoragePtr & table, const std::unordered_set<const IStorage *> & storages_with_action_locks)
{
    if (storages_with_action_locks.contains(table.get()))
        return false;

    /// A table of a database with `lazy_load_tables` is a proxy, look at the storage behind it.
    /// If it is not loaded yet, it has no data in memory.
    StoragePtr storage = table;
    if (const auto * proxy = dynamic_cast<const StorageProxy *>(table.get()); proxy && proxy->isLazyStandIn())
    {
        storage = proxy->tryGetNested();
        if (!storage)
            return true;
    }

    if (storage->isDictionary() || storage->isSystemStorage())
        return false;

    /// `SYSTEM STOP VIEW` and `SYSTEM PAUSE VIEW` do not hold an action lock, they change the state of the refresh task,
    /// and a reattached view would start refreshing again.
    if (const auto * materialized_view = dynamic_cast<const StorageMaterializedView *>(storage.get()); materialized_view && materialized_view->isRefreshable())
        return false;

    const String engine = storage->getName();
    /// `Buffer` keeps data in memory and flushes it on `DETACH`, which loses the data if there is no destination.
    if (engine == "Memory" || engine == "Buffer")
        return false;

    /// `Set` and `Join` with `persistent = 0` keep their data only in memory.
    if ((engine == "Set" || engine == "Join") && !storage->hasUnreplicatedTableDataOnDisk())
        return false;

    return true;
}

/// The internal queries run with full access, so require that the user could run them.
/// It also checks `readonly` and `allow_ddl`. Whether the table is a view is not known for a table which is not loaded yet,
/// so require the grants for both.
/// `ATTACH` also requires `TABLE ENGINE` on the engine from the metadata (see `InterpreterCreateQuery`),
/// so read the metadata as `ATTACH` does, which works for a table which is not loaded yet as well.
bool isGrantedToReattach(const DatabasePtr & database, const String & table_name, const ContextPtr & context, const ContextPtr & query_context)
{
    const String & database_name = database->getDatabaseName();
    const auto access = query_context->getAccess();

    const AccessFlags flags = AccessFlags(AccessType::DROP_TABLE) | AccessType::DROP_VIEW | AccessType::CREATE_TABLE | AccessType::CREATE_VIEW;
    if (!access->isGranted(flags, database_name, table_name))
        return false;

    const auto create_query = database->tryGetCreateTableQuery(table_name, context);
    if (!create_query)
        return false;

    const auto & create = create_query->as<const ASTCreateQuery &>();
    if (create.storage && create.storage->engine && !access->isGranted(AccessType::TABLE_ENGINE, create.storage->engine->name))
        return false;

    return true;
}

/// A table used by another query or DDL must not be reattached: `DETACH ... SYNC` would wait for that query to finish,
/// and in an `Atomic` database it shuts the storage down under the running query.
bool isIdle(const StoragePtr & table, const ContextPtr & context)
{
    /// Not zero: zero means an infinite wait.
    const Poco::Timespan probe_timeout(0, 1000);

    /// The queries using a lazily loaded table lock the storage behind it, not the stand-in.
    std::vector<StoragePtr> storages_to_probe{table};
    if (const auto * proxy = dynamic_cast<const StorageProxy *>(table.get()); proxy && proxy->isLazyStandIn())
        if (auto nested = proxy->tryGetNested())
            storages_to_probe.push_back(std::move(nested));

    try
    {
        for (const auto & storage : storages_to_probe)
            auto lock = storage->lockExclusively(context->getCurrentQueryId(), probe_timeout);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::DEADLOCK_AVOIDED || e.code() == ErrorCodes::TABLE_IS_DROPPED)
            return false;
        throw;
    }

    return DatabaseCatalog::instance().tryGetDDLGuardForStorage(table, probe_timeout) != nullptr;
}

void executeInternal(const String & query, const ContextPtr & context)
{
    /// A copy of the global context has full access and the server's default settings,
    /// so the result does not depend on the user and the settings of the outer query.
    auto internal_context = Context::createCopy(context->getGlobalContext());
    internal_context->makeQueryContext();
    internal_context->setCurrentQueryId({});

    /// Use a separate thread group, so that the work of the internal query is not accounted to the outer query:
    /// its memory tracker, profile events and threads stay as without reattaching.
    ThreadGroupSwitcher thread_group_switcher(ThreadGroup::createForQuery(internal_context), ThreadName::REATTACH_TABLES, /* allow_existing_group= */ true);

    auto io = executeQuery(query, internal_context, QueryFlags{.internal = true}).second;
    executeTrivialBlockIO(io, internal_context);
}

void reattachTables(const DatabasePtr & database, const std::vector<String> & table_names, const ContextPtr & context, const ContextPtr & query_context)
{
    auto log = getLogger("reattachRandomTables");
    const String & database_name = database->getDatabaseName();
    const auto storages_with_action_locks = getStoragesWithActionLocks(context);

    for (const auto & table_name : table_names)
    {
        {
            auto table = database->tryGetTable(table_name, context);
            if (!table
                || !isGrantedToReattach(database, table_name, context, query_context)
                || !canBeReattached(table, storages_with_action_locks)
                || !isIdle(table, context))
                continue;
        }

        const String quoted_name = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name);

        /// A failed `DETACH` leaves the table as it was (e.g. it was dropped concurrently), so skip it.
        try
        {
            executeInternal("DETACH TABLE " + quoted_name + " SYNC", context);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot detach table " + quoted_name + ", skipping it", LogsLevel::information);
            continue;
        }

        /// A failed `ATTACH` after a successful `DETACH` is what we are looking for, so let it fail the query.
        try
        {
            executeInternal("ATTACH TABLE " + quoted_name, context);
        }
        catch (Exception & e)
        {
            e.addMessage("While attaching table {} back (reattach_tables_before_query_execution_probability)", quoted_name);
            throw;
        }

        LOG_TEST(log, "Reattached table {}", quoted_name);
    }
}

}

void reattachRandomTablesInCurrentDatabase(const ContextPtr & context)
{
    const double probability = static_cast<double>(context->getSettingsRef()[Setting::reattach_tables_before_query_execution_probability].value);
    if (probability <= 0)
        return;

    /// A transaction keeps references to the storages it has modified until it is finished,
    /// so `DETACH ... SYNC` would wait for the `COMMIT` or `ROLLBACK` of the same session forever.
    if (context->getCurrentTransaction() || context->getSettingsRef()[Setting::implicit_transaction])
        return;

    const String database_name = context->getCurrentDatabase();
    if (database_name.empty() || DatabaseCatalog::isPredefinedDatabase(database_name))
        return;

    auto database = DatabaseCatalog::instance().tryGetDatabase(database_name);
    if (!database)
        return;

    /// Other engines either do not support a non-permanent `DETACH` (e.g. `Replicated`, `Memory`), or do not own their tables.
    const String database_engine = database->getEngineName();
    if (database_engine != "Atomic" && database_engine != "Ordinary")
        return;

    auto global_context = context->getGlobalContext();

    std::bernoulli_distribution distribution(std::min(probability, 1.0));
    std::vector<String> table_names;
    for (auto it = database->getTablesIterator(global_context, {}, /* skip_not_loaded= */ true); it->isValid(); it->next())
        if (distribution(thread_local_rng))
            table_names.push_back(it->name());

    if (table_names.empty())
        return;

    reattachTables(database, table_names, global_context, context);
}

}
