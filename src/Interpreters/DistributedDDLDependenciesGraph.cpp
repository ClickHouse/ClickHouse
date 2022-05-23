#include <Interpreters/DistributedDDLDependenciesGraph.h>
#include <Databases/IDatabase.h>
#include <Databases/DistributedDDLDependencyVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Poco/Util/AbstractConfiguration.h>
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
    log = &Poco::Logger::get("DistributedDDLDependenciesGraph");
}

void DependenciesGraph::resetState()
{
    completely_processed_tasks.clear();
    tasks_dependencies.independent_queries.clear();
    tasks_dependencies.dependencies_info.clear();
    tasks_dependencies.database_objects_in_query.clear();
    tasks_dependencies.total_queries = 0;
}

void DependenciesGraph::addTask(DDLTaskPtr && task)
{
    LOG_DEBUG(log, "Start adding task");
    auto name = task->entry_name;
    LOG_DEBUG(log, "Got name");
    if (tasks_dependencies.database_objects_in_query.contains(name))
        /// Skip if already added
        return;
    LOG_DEBUG(log, "Checked not contains(name)) in graph already");

    auto database_objects_for_added_task = getDependenciesSetFromQuery(global_context, task->query);
    LOG_DEBUG(log, "Got dependencies");

    if (database_objects_for_added_task.empty())
    {
        /// No dependencies means query can't have any dependencies or dependent queries
        tasks_dependencies.independent_queries.insert(name);
        LOG_DEBUG(log, "Added to independent");
        return;
    }
    tasks_dependencies.database_objects_in_query[name] = database_objects_for_added_task;
    for (const auto & [query_name, query_objects] : tasks_dependencies.database_objects_in_query)
    {
        if (name == query_name)
        {
            continue;
        }
        /// Check intersection of sets of database_objects (e.g. tables, databases, dictionaries)
        bool independent = true;
        for (const auto& database_object : database_objects_for_added_task)
        {
            if (query_objects.contains(database_object))
            {
                independent = false;
                break;
            }
        }
        if (!independent)
        {
            tasks_dependencies.dependencies_info[query_name].dependent_queries.insert(name);
            tasks_dependencies.dependencies_info[name].dependencies.insert(query_name);
        }
    }

    if (tasks_dependencies.dependencies_info[name].dependencies.empty())
    {
        tasks_dependencies.independent_queries.insert(name);
    }
}

QueryNamesSet DependenciesGraph::getTasksToParallelProcess()
{
    tasks_processed += tasks_dependencies.independent_queries.size();

    logDependencyGraph();
    removeProcessedTasks();

    return tasks_dependencies.independent_queries;
}

void DependenciesGraph::removeProcessedTasks()
{
    auto & old_independent_queries = tasks_dependencies.independent_queries;

    auto task_name_it = old_independent_queries.begin();

    while (task_name_it != old_independent_queries.end())
    {
        const auto & task_name = *task_name_it;
        if (completely_processed_tasks[task_name])
        {
            removeTask(task_name);
            task_name_it = old_independent_queries.erase(task_name_it);
            continue;
        }
        ++task_name_it;
    }
}

void DependenciesGraph::removeTask(String query_name)
{
    const QueriesDependenciesInfo & info = tasks_dependencies.dependencies_info[query_name];
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
            tasks_dependencies.independent_queries.insert(dependent_query);
        }
    }
    tasks_dependencies.dependencies_info.erase(query_name);
    tasks_dependencies.database_objects_in_query.erase(query_name);
    currently_processing_tasks.erase(query_name);
}

void DependenciesGraph::logDependencyGraph() const
{
    LOG_TEST(log, "Have {} independent queries.",
             tasks_dependencies.independent_queries.size());
    for (const auto & independent_query : tasks_dependencies.independent_queries)
    {
        const auto & query_dependencies = tasks_dependencies.dependencies_info.at(independent_query);

        LOG_TEST(log,
                 "Independent query: {} have {} dependencies and {} dependent queries.",
                 independent_query,
                 query_dependencies.dependencies.size(),
                 query_dependencies.dependent_queries.size());
    }
    for (const auto & dependencies : tasks_dependencies.dependencies_info)
    {
        LOG_TEST(log,
                 "Query {} have {} dependencies and {} dependent queries.",
                 dependencies.first,
                 dependencies.second.dependencies.size(),
                 dependencies.second.dependent_queries.size());
    }
}

}
