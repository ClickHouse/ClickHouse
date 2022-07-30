#include <Databases/TablesLoader.h>
#include <Databases/IDatabase.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int INFINITE_LOOP;
    extern const int LOGICAL_ERROR;
}

static constexpr size_t PRINT_MESSAGE_EACH_N_OBJECTS = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;

void mergeDependenciesGraphs(DependenciesInfos & main_dependencies_info, const DependenciesInfos & additional_info)
{
    for (const auto & table_and_info : additional_info)
    {
        const QualifiedTableName & table = table_and_info.first;
        const TableNamesSet & dependent_tables = table_and_info.second.dependent_database_objects;
        const TableNamesSet & dependencies = table_and_info.second.dependencies;

        DependenciesInfo & maybe_existing_info = main_dependencies_info[table];
        maybe_existing_info.dependent_database_objects.insert(dependent_tables.begin(), dependent_tables.end());
        if (!dependencies.empty())
        {
            if (maybe_existing_info.dependencies.empty())
                maybe_existing_info.dependencies = dependencies;
            else if (maybe_existing_info.dependencies != dependencies)
            {
                /// Can happen on DatabaseReplicated recovery
                LOG_WARNING(&Poco::Logger::get("TablesLoader"), "Replacing outdated dependencies ({}) of {} with: {}",
                            fmt::join(maybe_existing_info.dependencies, ", "),
                            table,
                            fmt::join(dependencies, ", "));
                for (const auto & old_dependency : maybe_existing_info.dependencies)
                {
                    [[maybe_unused]] bool removed = main_dependencies_info[old_dependency].dependent_database_objects.erase(table);
                    assert(removed);
                }
                maybe_existing_info.dependencies = dependencies;
            }
        }
    }
}

void logAboutProgress(Poco::Logger * log, size_t processed, size_t total, AtomicStopwatch & watch)
{
    if (processed % PRINT_MESSAGE_EACH_N_OBJECTS == 0 || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
    {
        LOG_INFO(log, "{}%", processed * 100.0 / total);
        watch.restart();
    }
}

TablesLoader::TablesLoader(ContextMutablePtr global_context_, Databases databases_, bool force_restore_, bool force_attach_)
: global_context(global_context_)
, databases(std::move(databases_))
, force_restore(force_restore_)
, force_attach(force_attach_)
{
    metadata.default_database = global_context->getCurrentDatabase();
    log = &Poco::Logger::get("TablesLoader");
}


void TablesLoader::loadTables()
{
    bool need_resolve_dependencies = !global_context->getConfigRef().has("ignore_table_dependencies_on_metadata_loading");

    /// Load all Lazy, MySQl, PostgreSQL, SQLite, etc databases first.
    for (auto & database : databases)
    {
        if (need_resolve_dependencies && database.second->supportsLoadingInTopologicalOrder())
            databases_to_load.push_back(database.first);
        else
            database.second->loadStoredObjects(global_context, force_restore, force_attach, true);
    }

    if (databases_to_load.empty())
        return;

    /// Read and parse metadata from Ordinary, Atomic, Materialized*, Replicated, etc databases. Build dependency graph.
    for (auto & database_name : databases_to_load)
    {
        databases[database_name]->beforeLoadingMetadata(global_context, force_restore, force_attach);
        databases[database_name]->loadTablesMetadata(global_context, metadata, force_attach);
    }

    LOG_INFO(log, "Parsed metadata of {} tables in {} databases in {} sec",
             metadata.parsed_tables.size(), databases_to_load.size(), stopwatch.elapsedSeconds());

    stopwatch.restart();

    logDependencyGraph();

    /// Remove tables that do not exist
    removeUnresolvableDependencies(/* remove_loaded */ false);

    /// Update existing info (it's important for ATTACH DATABASE)
    DatabaseCatalog::instance().addLoadingDependencies(metadata.dependencies_info);

    /// Some tables were loaded by database with loadStoredObjects(...). Remove them from graph if necessary.
    removeUnresolvableDependencies(/* remove_loaded */ true);

    loadTablesInTopologicalOrder(pool);
}

void TablesLoader::startupTables()
{
    /// Startup tables after all tables are loaded. Background tasks (merges, mutations, etc) may slow down data parts loading.
    for (auto & database : databases)
        database.second->startupTables(pool, force_restore, force_attach);
}


void TablesLoader::removeUnresolvableDependencies(bool remove_loaded)
{
    auto need_exclude_dependency = [this, remove_loaded](const QualifiedTableName & dependency_name, const DependenciesInfo & info)
    {
        /// Table exists and will be loaded
        if (metadata.parsed_tables.contains(dependency_name))
            return false;
        /// Table exists and it's already loaded
        if (DatabaseCatalog::instance().isTableExist(StorageID(dependency_name.database, dependency_name.table), global_context))
            return remove_loaded;
        /// It's XML dictionary.
        if (dependency_name.database == metadata.default_database &&
            global_context->getExternalDictionariesLoader().has(dependency_name.table))
        {
            LOG_WARNING(log, "Tables {} depend on XML dictionary {}, but XML dictionaries are loaded independently."
                        "Consider converting it to DDL dictionary.", fmt::join(info.dependent_database_objects, ", "), dependency_name);
            return true;
        }

        /// Some tables depends on table "dependency_name", but there is no such table in DatabaseCatalog and we don't have its metadata.
        /// We will ignore it and try to load dependent tables without "dependency_name"
        /// (but most likely dependent tables will fail to load).
        LOG_WARNING(log, "Tables {} depend on {}, but seems like the it does not exist. Will ignore it and try to load existing tables",
                    fmt::join(info.dependent_database_objects, ", "), dependency_name);

        if (!info.dependencies.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} does not exist, but we have seen its AST and found {} dependencies."
                                                       "It's a bug", dependency_name, info.dependencies.size());
        if (info.dependent_database_objects.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} does not have dependencies and dependent tables as it expected to."
                                                       "It's a bug", dependency_name);

        return true;
    };

    auto table_it = metadata.dependencies_info.begin();
    while (table_it != metadata.dependencies_info.end())
    {
        auto & info = table_it->second;
        if (need_exclude_dependency(table_it->first, info))
            table_it = removeResolvedDependency(table_it, metadata.independent_database_objects);
        else
            ++table_it;
    }
}

void TablesLoader::loadTablesInTopologicalOrder(ThreadPool & pool)
{
    /// Compatibility setting which should be enabled by default on attach
    /// Otherwise server will be unable to start for some old-format of IPv6/IPv4 types of columns
    ContextMutablePtr load_context = Context::createCopy(global_context);
    load_context->setSetting("cast_ipv4_ipv6_default_on_conversion_error", 1);

    /// Load independent tables in parallel.
    /// Then remove loaded tables from dependency graph, find tables/dictionaries that do not have unresolved dependencies anymore,
    /// move them to the list of independent tables and load.
    /// Repeat until we have some tables to load.
    /// If we do not, then either all objects are loaded or there is cyclic dependency.
    /// Complexity: O(V + E)
    size_t level = 0;
    do
    {
        assert(metadata.parsed_tables.size() == tables_processed + metadata.independent_database_objects.size() + getNumberOfTablesWithDependencies());
        logDependencyGraph();

        startLoadingIndependentTables(pool, level, load_context);

        TableNames new_independent_database_objects;
        for (const auto & table_name : metadata.independent_database_objects)
        {
            auto info_it = metadata.dependencies_info.find(table_name);
            if (info_it == metadata.dependencies_info.end())
            {
                /// No tables depend on table_name and it was not even added to dependencies_info
                continue;
            }
            removeResolvedDependency(info_it, new_independent_database_objects);
        }

        pool.wait();

        metadata.independent_database_objects = std::move(new_independent_database_objects);
        ++level;
    } while (!metadata.independent_database_objects.empty());

    checkCyclicDependencies();
}

DependenciesInfosIter TablesLoader::removeResolvedDependency(const DependenciesInfosIter & info_it, TableNames & independent_database_objects)
{
    const QualifiedTableName & table_name = info_it->first;
    const DependenciesInfo & info = info_it->second;
    if (!info.dependencies.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} is in list of independent tables, but dependencies count is {}."
                                                   "It's a bug", table_name, info.dependencies.size());
    if (info.dependent_database_objects.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} does not have dependent tables. It's a bug", table_name);

    /// Decrement number of dependencies for each dependent table
    for (const auto & dependent_table : info.dependent_database_objects)
    {
        auto & dependent_info = metadata.dependencies_info[dependent_table];
        auto & dependencies_set = dependent_info.dependencies;
        if (dependencies_set.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to decrement 0 dependencies counter for {}. It's a bug", dependent_table);
        if (!dependencies_set.erase(table_name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot remove {} from dependencies set of {}, it contains only {}",
                            table_name, dependent_table, fmt::join(dependencies_set, ", "));
        if (dependencies_set.empty())
        {
            independent_database_objects.push_back(dependent_table);
            if (dependent_info.dependent_database_objects.empty())
                metadata.dependencies_info.erase(dependent_table);
        }
    }

    return metadata.dependencies_info.erase(info_it);
}

void TablesLoader::startLoadingIndependentTables(ThreadPool & pool, size_t level, ContextMutablePtr load_context)
{
    size_t total_tables = metadata.parsed_tables.size();

    LOG_INFO(log, "Loading {} tables with {} dependency level", metadata.independent_database_objects.size(), level);

    for (const auto & table_name : metadata.independent_database_objects)
    {
        pool.scheduleOrThrowOnError([this, load_context, total_tables, &table_name]()
        {
            const auto & path_and_query = metadata.parsed_tables[table_name];
            databases[table_name.database]->loadTableFromMetadata(load_context, path_and_query.path, table_name, path_and_query.ast, force_restore);
            logAboutProgress(log, ++tables_processed, total_tables, stopwatch);
        });
    }
}

size_t TablesLoader::getNumberOfTablesWithDependencies() const
{
    size_t number_of_tables_with_dependencies = 0;
    for (const auto & info : metadata.dependencies_info)
        if (!info.second.dependencies.empty())
            ++number_of_tables_with_dependencies;
    return number_of_tables_with_dependencies;
}

void TablesLoader::checkCyclicDependencies() const
{
    /// Loading is finished if all dependencies are resolved
    if (metadata.dependencies_info.empty())
        return;

    for (const auto & info : metadata.dependencies_info)
    {
        LOG_WARNING(log, "Cannot resolve dependencies: Table {} have {} dependencies and {} dependent tables. List of dependent tables: {}",
                    info.first, info.second.dependencies.size(),
                    info.second.dependent_database_objects.size(), fmt::join(info.second.dependent_database_objects, ", "));
        assert(info.second.dependencies.empty());
    }

    throw Exception(ErrorCodes::INFINITE_LOOP, "Cannot attach {} tables due to cyclic dependencies. "
                                               "See server log for details.", metadata.dependencies_info.size());
}

void TablesLoader::logDependencyGraph() const
{
    LOG_TEST(log, "Have {} independent tables: {}",
              metadata.independent_database_objects.size(),
              fmt::join(metadata.independent_database_objects, ", "));
    for (const auto & dependencies : metadata.dependencies_info)
    {
        LOG_TEST(log,
            "Table {} have {} dependencies and {} dependent tables. List of dependent tables: {}",
            dependencies.first,
            dependencies.second.dependencies.size(),
            dependencies.second.dependent_database_objects.size(),
            fmt::join(dependencies.second.dependent_database_objects, ", "));
    }
}

}
