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

DependenciesGraph::DependenciesGraph(ContextPtr global_context_)
        : global_context(Context::createCopy(global_context_))
{
    tasks_dependencies.default_database = global_context->getCurrentDatabase();
    log = &Poco::Logger::get("DistributedDDLDependenciesGraph");
}

void DependenciesGraph::addTask(DDLTaskPtr & task)
{
    tasks_dependencies.total_queries++;
    auto name = task->entry_name;
    auto database_objects_for_added_task = getDependenciesSetFromQuery(global_context, task->query);
    tasks_dependencies.database_objects_in_query[name] = database_objects_for_added_task;
    for (const auto & [query_name, query_objects] : tasks_dependencies.database_objects_in_query)
    {
        if (name == query_name)
        {
            continue;
        }
        /// Check intersection of sets of database_objects(e.g. tables, databases, dictionaries)
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
        completely_processed_tasks[name] = false;
        tasks_dependencies.independent_queries.push_back(name);
    }
}

QueryNames DependenciesGraph::getTasksToParallelProcess()
{
    tasks_processed += tasks_dependencies.independent_queries.size();

    logDependencyGraph();

    return tasks_dependencies.independent_queries;
}

void DependenciesGraph::removeProcessedTasks()
{
    auto & old_independent_queries = tasks_dependencies.independent_queries;
    QueryNames new_independent_queries;

    for (const auto& task_name : old_independent_queries)
    {
        if (completely_processed_tasks[task_name])
            removeTask(task_name);
        else
            new_independent_queries.push_back(task_name);
    }

    old_independent_queries = new_independent_queries;
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
            tasks_dependencies.independent_queries.emplace_back(dependent_query);
            if (dependent_info.dependent_queries.empty())
                tasks_dependencies.dependencies_info.erase(dependent_query);
        }
    }

    tasks_dependencies.dependencies_info.erase(query_name);
}

void DependenciesGraph::logDependencyGraph() const
{
    LOG_TEST(log, "Have {} independent queries.",
             tasks_dependencies.independent_queries.size());
    for (const auto & independent_query : tasks_dependencies.independent_queries)
    {
        LOG_TEST(log,
                 "Independent query {} have {} dependencies and {} dependent queries, completely_processed={}.",
                 independent_query,
                 tasks_dependencies.dependencies_info[independent_query].dependencies.size(),
                 tasks_dependencies.dependencies_info[independent_query].dependent_queries.size(),
                 completely_processed_tasks[independent_query]);
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
