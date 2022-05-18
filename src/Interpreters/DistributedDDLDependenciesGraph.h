#pragma once
#include <Core/Types.h>
#include <Core/QualifiedTableName.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DDLTask.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <mutex>


namespace Poco
{
class Logger;
}

class AtomicStopwatch;

namespace DB
{

using DDLTaskPtr = std::unique_ptr<DDLTaskBase>;
using TableNames = std::vector<QualifiedTableName>;
using TableNamesSet = std::unordered_set<QualifiedTableName>;
using QueryNamesSet = std::unordered_set<String>;

struct QueriesDependenciesInfo
{
    /// Set of dependencies
    QueryNamesSet dependencies;
    /// Set of tables/dictionaries which depend on this table/dictionary
    QueryNamesSet dependent_queries;
};

using QueriesDependenciesInfos = std::unordered_map<String, QueriesDependenciesInfo>; /// entry_name -> Dependencies_queries
using DatabaseObjectsInAST = std::unordered_map<String, TableNamesSet>;
using ProcessInfo = std::unordered_map<String, bool>;

struct TasksDependencies
{
    String default_database;

    /// For logging
    size_t total_queries = 0;

    /// Set of tables/dictionaries that do not have any dependencies and can be loaded
    QueryNamesSet independent_queries;
    /// Adjacent list of dependency graph, contains two maps
    /// 2. query name -> dependent queries list (adjacency list of dependencies graph).
    /// 1. query name -> dependencies of queries (adjacency list of inverted dependencies graph)
    /// If query A depends on query B, then there is an edge B --> A, i.e. dependencies_info[B].dependent_database_objects contains A
    /// and dependencies_info[A].dependencies contain B.
    /// We need inverted graph to effectively maintain it on DDL queries that can modify the graph.
    QueriesDependenciesInfos dependencies_info;

    /// Map of sets of table/database/dictionary names on which query depends
    DatabaseObjectsInAST database_objects_in_query;
};

class DependenciesGraph
{
public:

    ProcessInfo completely_processed_tasks;
    QueryNamesSet currently_processing_tasks;

    DependenciesGraph(ContextMutablePtr global_context_);
    DependenciesGraph() = delete;

    void addTask(DDLTaskPtr && task);

    void removeTask(String query_name);

    void removeProcessedTasks();

    void resetState();

    QueryNamesSet getTasksToParallelProcess();

private:
    ContextMutablePtr global_context;

    TasksDependencies tasks_dependencies;
    Poco::Logger * log;
    size_t tasks_processed{0};
    AtomicStopwatch stopwatch;

    void logDependencyGraph() const;
};

}
