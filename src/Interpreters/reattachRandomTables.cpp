#include <Interpreters/reattachRandomTables.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/thread_local_rng.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Storages/IStorage.h>

#include <random>


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
}

namespace
{

/// Reattaching these tables would change what the queries see, which is not a bug.
bool canBeReattached(const StoragePtr & table, const ContextPtr & context)
{
    if (table->isDictionary() || table->isSystemStorage())
        return false;

    const String engine = table->getName();
    /// `Buffer` keeps data in memory and flushes it on `DETACH`, which loses the data if there is no destination.
    if (engine == "Memory" || engine == "Buffer")
        return false;

    /// `Set` and `Join` with `persistent = 0` keep their data only in memory.
    if ((engine == "Set" || engine == "Join") && !table->hasUnreplicatedTableDataOnDisk())
        return false;

    /// The action locks (e.g. from `SYSTEM STOP MERGES`) belong to the storage object and are lost on reattach.
    if (context->getActionLocksManager()->hasAny(table))
        return false;

    return true;
}

/// A table used by another query or DDL must not be reattached: `DETACH ... SYNC` would wait for that query to finish,
/// and in an `Atomic` database it shuts the storage down under the running query.
bool isIdle(const StoragePtr & table, const ContextPtr & context)
{
    /// Not zero: zero means an infinite wait.
    const Poco::Timespan probe_timeout(0, 1000);

    try
    {
        auto lock = table->lockExclusively(context->getCurrentQueryId(), probe_timeout);
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

void reattachTables(const DatabasePtr & database, const std::vector<String> & table_names, const ContextPtr & context)
{
    auto log = getLogger("reattachRandomTables");
    const String & database_name = database->getDatabaseName();

    for (const auto & table_name : table_names)
    {
        {
            auto table = database->tryGetTable(table_name, context);
            if (!table || !canBeReattached(table, context) || !isIdle(table, context))
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

    reattachTables(database, table_names, global_context);
}

}
