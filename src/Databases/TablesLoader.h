#pragma once
#include <Core/Types.h>
#include <Core/QualifiedTableName.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
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


class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;

using ParsedMetadata = std::map<QualifiedTableName, std::pair<String, ASTPtr>>;
using TableNames = std::vector<QualifiedTableName>;

struct DependenciesInfo
{
    /// How many dependencies this table have
    size_t dependencies_count = 0;
    /// List of tables which depend on this table
    TableNames dependent_tables;
};

using DependenciesInfos = std::unordered_map<QualifiedTableName, DependenciesInfo>;
using DependenciesInfosIter = std::unordered_map<QualifiedTableName, DependenciesInfo>::iterator;

struct ParsedTablesMetadata
{
    String default_database;

    std::mutex mutex;
    ParsedMetadata metadata;

    /// For logging
    size_t total_dictionaries = 0;

    /// List of tables that do not have any dependencies and can be loaded
    TableNames independent_tables;

    /// Actually it contains two different maps (with, probably, intersecting keys):
    /// 1. table name -> number of dependencies
    /// 2. table name -> dependent tables list (adjacency list of dependencies graph).
    /// If table A depends on table B, then there is an edge B --> A, i.e. dependencies_info[B].dependent_tables contains A.
    /// And dependencies_info[C].dependencies_count is a number of incoming edges for vertex C (how many tables we have to load before C).
    DependenciesInfos dependencies_info;
};

/// Loads tables (and dictionaries) from specified databases
/// taking into account dependencies between them.
class TablesLoader
{
public:
    using Databases = std::map<String, DatabasePtr>;

    TablesLoader(ContextMutablePtr global_context_, Databases databases_, bool force_restore_ = false, bool force_attach_ = false);
    TablesLoader() = delete;

    void loadTables();
    void startupTables();

private:
    ContextMutablePtr global_context;
    Databases databases;
    bool force_restore;
    bool force_attach;

    Strings databases_to_load;
    ParsedTablesMetadata all_tables;
    Poco::Logger * log;
    std::atomic<size_t> tables_processed{0};
    AtomicStopwatch stopwatch;

    ThreadPool pool;

    void removeUnresolvableDependencies();

    void loadTablesInTopologicalOrder(ThreadPool & pool);

    DependenciesInfosIter removeResolvedDependency(const DependenciesInfosIter & info_it, TableNames & independent_tables);

    void startLoadingIndependentTables(ThreadPool & pool, size_t level);

    void checkCyclicDependencies() const;

    size_t getNumberOfTablesWithDependencies() const;

    void logDependencyGraph() const;
};

}
