#include <Databases/TablesLoader.h>

namespace DB
{

static constexpr size_t PRINT_MESSAGE_EACH_N_OBJECTS = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;


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
    all_tables.default_database = global_context->getCurrentDatabase();
    log = &Poco::Logger::get("TablesLoader");
}


void TablesLoader::loadTables()
{
    for (auto & database : databases)
    {
        if (database->supportsLoadingInTopologicalOrder())
            databases_to_load.emplace(database->getDatabaseName(), database);
        else
            database->loadStoredObjects(global_context, force_restore, force_attach, true);
    }

    for (auto & database : databases_to_load)
        database.second->loadTablesMetadata(global_context, all_tables);

    auto table_does_not_exist = [&](const QualifiedTableName & table_name, const QualifiedTableName & dependency_name)
        {
        if (all_tables.metadata.contains(dependency_name))
            return false;
        if (DatabaseCatalog::instance().isTableExist(StorageID(dependency_name.database, dependency_name.table), global_context))
            return false;
        /// FIXME if XML dict

        LOG_WARNING(log, "Table {} depends on {}, but seems like the second one does not exist", table_name, dependency_name);
        return true;
        };

    removeDependencies(table_does_not_exist, all_tables.independent_tables);

    //LOG_TRACE(log, "Independent database objects: {}", fmt::join(all_tables.independent_tables, ", "));
    //for (const auto & dependencies : all_tables.table_dependencies)
    //    LOG_TRACE(log, "Database object {} depends on: {}", dependencies.first, fmt::join(dependencies.second, ", "));

    auto is_dependency_loaded = [&](const QualifiedTableName & /*table_name*/, const QualifiedTableName & dependency_name)
        {
        return all_tables.independent_tables.contains(dependency_name);
        };

    AtomicStopwatch watch;
    ThreadPool pool;
    size_t level = 0;
    do
    {
        assert(all_tables.metadata.size() == tables_processed + all_tables.independent_tables.size() + all_tables.table_dependencies.size());
        startLoadingIndependentTables(pool, watch, level);
        std::unordered_set<QualifiedTableName> new_independent_tables;
        removeDependencies(is_dependency_loaded, new_independent_tables);
        pool.wait();
        all_tables.independent_tables = std::move(new_independent_tables);
        checkCyclicDependencies();
        ++level;
        assert(all_tables.metadata.size() == tables_processed + all_tables.independent_tables.size() + all_tables.table_dependencies.size());
    } while (!all_tables.independent_tables.empty());

    for (auto & database : databases_to_load)
    {
        database.second->startupTables();
    }
}

void TablesLoader::removeDependencies(RemoveDependencyPredicate need_remove_dependency, std::unordered_set<QualifiedTableName> & independent_tables)
{
    auto table_it = all_tables.table_dependencies.begin();
    while (table_it != all_tables.table_dependencies.end())
    {
        auto & dependencies = table_it->second;
        assert(!dependencies.empty());
        auto dependency_it = dependencies.begin();
        while (dependency_it != dependencies.end())
        {
            if (need_remove_dependency(table_it->first, *dependency_it))
                dependency_it = dependencies.erase(dependency_it);
            else
                ++dependency_it;
        }

        if (dependencies.empty())
        {
            independent_tables.emplace(std::move(table_it->first));
            table_it = all_tables.table_dependencies.erase(table_it);
        }
        else
        {
            ++table_it;
        }
    }
}

void TablesLoader::startLoadingIndependentTables(ThreadPool & pool, AtomicStopwatch & watch, size_t level)
{
    size_t total_tables = all_tables.metadata.size();

    LOG_INFO(log, "Loading {} tables with {} dependency level", all_tables.independent_tables.size(), level);

    for (const auto & table_name : all_tables.independent_tables)
    {
        pool.scheduleOrThrowOnError([&]()
        {
            const auto & path_and_query = all_tables.metadata[table_name];
            const auto & path = path_and_query.first;
            const auto & ast = path_and_query.second;
            databases_to_load[table_name.database]->loadTableFromMetadata(global_context, path, table_name, ast, force_restore);
            logAboutProgress(log, ++tables_processed, total_tables, watch);
        });
    }
}

void TablesLoader::checkCyclicDependencies() const
{
    if (!all_tables.independent_tables.empty())
        return;
    if (all_tables.table_dependencies.empty())
        return;

    for (const auto & dependencies : all_tables.table_dependencies)
    {
        LOG_WARNING(log, "Cannot resolve dependencies: Table {} depends on {}",
                    dependencies.first, fmt::join(dependencies.second, ", "));
    }

    throw Exception(ErrorCodes::INFINITE_LOOP, "Cannot attach {} tables due to cyclic dependencies. "
                                               "See server log for details.", all_tables.table_dependencies.size());
}

}

