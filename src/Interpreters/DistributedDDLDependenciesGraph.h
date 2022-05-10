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

void logAboutProgress(Poco::Logger * log, size_t processed, size_t total, AtomicStopwatch & watch);

using DDLTaskPtr = std::unique_ptr<DDLTaskBase>;
using TableNames = std::vector<QualifiedTableName>;
using Queries = std::list<DDLTaskPtr>;
using TableNamesSet = std::unordered_set<QualifiedTableName>;
using EntryNamesSet = std::unordered_set<String>;
using EntryNameToDDLTaskPtrMap = std::unordered_map<String, DDLTaskBase&>;

struct QueriesDependenciesInfo
{
    /// Set of dependencies
    EntryNamesSet dependencies;
    /// Set of tables/dictionaries which depend on this table/dictionary
    EntryNamesSet dependent_queries;
};

using QueriesDependenciesInfos = std::unordered_map<String, QueriesDependenciesInfo>; /// entry_name -> Dependencies_queries
using QueriesDependenciesInfosIter = std::unordered_map<String, QueriesDependenciesInfo>::iterator;
using DatabaseObjectsInAST = std::unordered_map<String, TableNamesSet>;

struct TasksDependencies
{
    String default_database;

    std::mutex mutex;

    /// For logging
    size_t total_dictionaries = 0;

    /// List of tables/dictionaries that do not have any dependencies and can be loaded
    Queries independent_queries;
    EntryNameToDDLTaskPtrMap name_to_ddl_task;
    /// Adjacent list of dependency graph, contains two maps
    /// 2. query name -> dependent queries list (adjacency list of dependencies graph).
    /// 1. query name -> dependencies of queries (adjacency list of inverted dependencies graph)
    /// If query A depends on query B, then there is an edge B --> A, i.e. dependencies_info[B].dependent_database_objects contains A
    /// and dependencies_info[A].dependencies contain B.
    /// We need inverted graph to effectively maintain it on DDL queries that can modify the graph.
    QueriesDependenciesInfos dependencies_info;
    DatabaseObjectsInAST database_objects_in_query;
};

class DependenciesGraph
{
public:

    TasksDependencies tasks_dependencies;

    DependenciesGraph(ContextMutablePtr global_context_);
    DependenciesGraph() = delete;

    void addTask(DDLTaskPtr & task);

    void removeTask(String entry_name);

    void removeProcessedTasks();

    Queries getTasksToParallelProcess();

private:
    ContextMutablePtr global_context;

    Poco::Logger * log;
    size_t tasks_processed{0};
    AtomicStopwatch stopwatch;

    void logDependencyGraph() const;
};

}
