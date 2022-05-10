#include <Interpreters/DistributedDDLDependenciesGraph.h>
#include <Databases/IDatabase.h>
#include <Databases/DistributedDDLDependencyVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <base/logger_useful.h>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

DependenciesGraph::DependenciesGraph(ContextMutablePtr global_context_)
        : global_context(global_context_)
{
    tasks_dependencies.default_database = global_context->getCurrentDatabase();
    log = &Poco::Logger::get("DistributedDDLDependenciesGraph");
}

void DependenciesGraph::addTask(DDLTaskPtr task)
{
    auto name = task->entry_name;
    name_to_ddl_task[name] = task;
    database_objects_for_added_task = getDependenciesSetFromQuery(global_context, task->query);
    tasks_dependencies.database_objects_in_query[name] = database_objects_for_added_task;
    for (auto & [query_name, query_objects] : tasks_dependencies.database_objects_in_query) {
        if (name == query_name)
        {
            continue;
        }
        /// Check intersection for O(tasks_dependencies.database_objects_in_query[name].size()))
        bool independent = true;
        for (const auto& database_object : database_objects_for_added_task)
        {
            if (query_objects.contains(database_object))
            {
                independent = false;
                break;
            }
        }
        if (!independent) {
            tasks_dependencies.dependencies_info[query_name].dependent_queries.insert(name);
            tasks_dependencies.dependencies_info[name].dependencies.insert(query_name);
        }
    }

    if (tasks_dependencies.dependencies_info[name].dependencies.emoty())
    {
        tasks_dependencies.independent_queries.insert(tasks_dependencies.name_to_ddl_task[name]);
    }
}

Queries DependenciesGraph::getTasksToParallelProcess()
{
    for (const auto & task : tasks_dependencies.independent_queries)
    {
        tasks_processed++;
        if (task->completely_processed.load())
        {
            removeTask(task->entry_name);
        }
    }

    logDependencyGraph();

    return tasks_dependencies.independent_queries;
}

void DependenciesGraph::removeProcessedTasks(Queries & old_independent_queries)
{

    Queries new_independent_queries;
    for (const auto& task : old_independent_queries)
    {
        if (task->completely_processed.load())
        {
            removeTask(task->entry_name);
        }
        else
        {
            new_independent_queries.insert(task);
        }
    }

    tasks_dependencies.independent_queries = new_independent_queries;
}

void DependenciesGraph::removeTask(String query_name)
{
    const DependenciesInfo & info = tasks_dependencies.dependencies_info[query_name];
    if (!info.dependencies.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query {} is in list of independent queries, but dependencies count is {}."
                                                   "It's a bug", query_name, info.dependencies.size());

    /// Decrement number of dependencies for each dependent query
    for (const auto & dependent_query : info.dependent_queries)
    {
        auto & dependent_info = tasks_dependencies.dependencies_info[dependent_query];
        auto & dependencies_set = dependent_info.dependencies;
        if (dependencies_set.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to decrement 0 dependencies counter for {}. It's a bug", dependent_query);
        if (!dependencies_set.erase(query_name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot remove {} from dependencies set of {}, it contains only {}",
                            query_name, dependent_query, fmt::join(dependencies_set, ", "));
        if (dependencies_set.empty())
        {
            independent_queries.insert(tasks_dependencies.name_to_ddl_task[dependent_query]);
            if (dependent_info.dependent_queries.empty())
                tasks_dependencies.dependencies_info.erase(dependent_query);
        }
    }
}


void DependenciesGraph::logDependencyGraph() const
{
    LOG_TEST(log, "Have {} independent queries: {}",
             tasks_dependencies.independent_queries.size(),
             fmt::join(tasks_dependencies.independent_queries, ", "));
    for (const auto & dependencies : tasks_dependencies.dependencies_info)
    {
        LOG_TEST(log,
                 "Query {} have {} dependencies and {} dependent queries. List of dependent queries: {}",
                 dependencies.first,
                 dependencies.second.dependencies.size(),
                 dependencies.second.dependent_queries.size(),
                 fmt::join(dependencies.second.dependent_queries, ", "));
    }
}

}
