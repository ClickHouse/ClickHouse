#include <Databases/TablesLoader.h>
#include <Databases/IDatabase.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DDLLoadingDependencyVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

TablesLoader::TablesLoader(ContextMutablePtr global_context_, Databases databases_, LoadingStrictnessLevel strictness_mode_)
    : global_context(global_context_)
    , databases(std::move(databases_))
    , strictness_mode(strictness_mode_)
    , referential_dependencies("ReferentialDeps")
    , loading_dependencies("LoadingDeps")
    , all_loading_dependencies("LoadingDeps")
    , async_loader(global_context->getAsyncLoader())
{
    metadata.default_database = global_context->getCurrentDatabase();
    log = getLogger("TablesLoader");
}

LoadTaskPtrs TablesLoader::loadTablesAsync(LoadJobSet load_after)
{
    bool need_resolve_dependencies = !global_context->getConfigRef().has("ignore_table_dependencies_on_metadata_loading");

    /// Load all Lazy, MySQL, PostgreSQL, SQLite, etc databases first.
    /// Note that this loading is NOT async because it should be fast and it cannot have any dependencies
    for (auto & database : databases)
    {
        if (need_resolve_dependencies && database.second->supportsLoadingInTopologicalOrder())
            databases_to_load.push_back(database.first);
        else
            database.second->loadStoredObjects(global_context, strictness_mode);
    }

    if (databases_to_load.empty())
        return {};

    LoadTaskPtrs result;

    /// Read and parse metadata from Ordinary, Atomic, Materialized*, Replicated, etc databases. Build dependency graph.
    for (auto & database_name : databases_to_load)
    {
        databases[database_name]->beforeLoadingMetadata(global_context, strictness_mode);
        bool is_startup = LoadingStrictnessLevel::FORCE_ATTACH <= strictness_mode;
        databases[database_name]->loadTablesMetadata(global_context, metadata, is_startup);
    }

    LOG_INFO(log, "Parsed metadata of {} tables in {} databases in {} sec",
             metadata.parsed_tables.size(), databases_to_load.size(), stopwatch.elapsedSeconds());

    stopwatch.restart();

    buildDependencyGraph();

    /// Update existing info (it's important for ATTACH DATABASE)
    DatabaseCatalog::instance().addDependencies(referential_dependencies, loading_dependencies);

    /// Remove tables that do not exist
    removeUnresolvableDependencies();

    /// Compatibility setting which should be enabled by default on attach
    /// Otherwise server will be unable to start for some old-format of IPv6/IPv4 types of columns
    ContextMutablePtr load_context = Context::createCopy(global_context);
    load_context->setSetting("cast_ipv4_ipv6_default_on_conversion_error", 1);

    for (const auto & table_id : all_loading_dependencies.getTablesSortedByDependency())
    {
        /// Gather tasks to load before this table
        LoadTaskPtrs load_dependency_tasks;
        for (const StorageID & dependency_id : all_loading_dependencies.getDependencies(table_id))
            load_dependency_tasks.push_back(load_table[dependency_id.getFullTableName()]);

        // Make load table task
        auto table_name = table_id.getQualifiedName();
        const auto & path_and_query = metadata.parsed_tables[table_name];
        auto task = databases[table_name.database]->loadTableFromMetadataAsync(
            async_loader,
            getGoals(load_dependency_tasks, load_after),
            load_context,
            path_and_query.path,
            table_name,
            path_and_query.ast,
            strictness_mode);
        load_table[table_id.getFullTableName()] = task;
        result.push_back(task);
    }

    return result;
}

LoadTaskPtrs TablesLoader::startupTablesAsync(LoadJobSet startup_after)
{
    LoadTaskPtrs result;
    std::unordered_map<String, LoadTaskPtrs> startup_database; /// database name -> all its tables startup tasks

    for (const auto & table_id : all_loading_dependencies.getTables())
    {
        // Make startup table task
        auto table_name = table_id.getQualifiedName();
        auto task = databases[table_name.database]->startupTableAsync(
            async_loader,
            joinJobs(load_table[table_id.getFullTableName()]->goals(), startup_after),
            table_name,
            strictness_mode);
        startup_database[table_name.database].push_back(task);
        result.push_back(task);
    }

    /// Make startup database tasks
    for (auto & database_name : databases_to_load)
    {
        auto task = databases[database_name]->startupDatabaseAsync(
            async_loader,
            getGoals(startup_database[database_name], startup_after),
            strictness_mode);
        result.push_back(task);
    }

    return result;
}

void TablesLoader::buildDependencyGraph()
{
    for (const auto & [table_name, table_metadata] : metadata.parsed_tables)
    {
        auto new_ref_dependencies = getDependenciesFromCreateQuery(global_context, table_name, table_metadata.ast, global_context->getCurrentDatabase());
        auto new_loading_dependencies = getLoadingDependenciesFromCreateQuery(global_context, table_name, table_metadata.ast);

        if (!new_ref_dependencies.empty())
            referential_dependencies.addDependencies(table_name, new_ref_dependencies);

        if (!new_loading_dependencies.empty())
            loading_dependencies.addDependencies(table_name, new_loading_dependencies);

        /// We're adding `new_loading_dependencies` to the graph here even if they're empty because
        /// we need to have all tables from `metadata.parsed_tables` in the graph.
        all_loading_dependencies.addDependencies(table_name, new_loading_dependencies);
    }

    referential_dependencies.log();
    all_loading_dependencies.log();
}

void TablesLoader::removeUnresolvableDependencies()
{
    auto need_exclude_dependency = [this](const StorageID & table_id)
    {
        /// Table exists and will be loaded
        if (metadata.parsed_tables.contains(table_id.getQualifiedName()))
            return false;

        if (DatabaseCatalog::instance().isTableExist(table_id, global_context))
        {
            /// Table exists and it's already loaded
        }
        else if (table_id.database_name == metadata.default_database &&
            global_context->getExternalDictionariesLoader().has(table_id.table_name))
        {
            /// Tables depend on a XML dictionary.
            LOG_WARNING(
                log,
                "Tables {} depend on XML dictionary {}, but XML dictionaries are loaded independently."
                "Consider converting it to DDL dictionary.",
                fmt::join(all_loading_dependencies.getDependents(table_id), ", "),
                table_id);
        }
        else
        {
            /// Some tables depend on table "table_id", but there is no such table in DatabaseCatalog and we don't have its metadata.
            /// We will ignore it and try to load dependent tables without "table_id"
            /// (but most likely dependent tables will fail to load).
            LOG_WARNING(
                log,
                "Tables {} depend on {}, but seems like that does not exist. Will ignore it and try to load existing tables",
                fmt::join(all_loading_dependencies.getDependents(table_id), ", "),
                table_id);
        }

        size_t num_dependencies, num_dependents;
        all_loading_dependencies.getNumberOfAdjacents(table_id, num_dependencies, num_dependents);
        if (num_dependencies || !num_dependents)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} does not have dependencies and dependent tables as it expected to."
                                                       "It's a bug", table_id);

        return true; /// Exclude this dependency.
    };

    all_loading_dependencies.removeTablesIf(need_exclude_dependency); // NOLINT

    if (all_loading_dependencies.getNumberOfTables() != metadata.parsed_tables.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of tables to be loaded is not as expected. It's a bug");

    /// Cannot load tables with cyclic dependencies.
    all_loading_dependencies.checkNoCyclicDependencies();
}

}
