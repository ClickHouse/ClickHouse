#pragma once

#include <map>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <Core/QualifiedTableName.h>
#include <Core/Types.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Databases/TablesDependencyGraph.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Common/Stopwatch.h>
#include <Common/AsyncLoader.h>


namespace Poco
{
    class Logger; // NOLINT(cppcoreguidelines-virtual-class-destructor)
}

class AtomicStopwatch;

namespace DB
{

class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;

struct ParsedTableMetadata
{
    String path;
    ASTPtr ast;
};

using ParsedMetadata = std::map<QualifiedTableName, ParsedTableMetadata>;

struct ParsedTablesMetadata
{
    String default_database;

    std::mutex mutex;
    ParsedMetadata parsed_tables;

    /// For logging
    size_t total_dictionaries = 0;
};

/// Loads tables (and dictionaries) from specified databases
/// taking into account dependencies between them.
class TablesLoader
{
public:
    using Databases = std::map<String, DatabasePtr>;

    TablesLoader(ContextMutablePtr global_context_, Databases databases_, LoadingStrictnessLevel strictness_mode_);
    TablesLoader() = delete;

    /// Create tasks for async loading of all tables in `databases` after specified jobs `load_after`.
    [[nodiscard]] LoadTaskPtrs loadTablesAsync(LoadJobSet load_after = {});

    /// Create tasks for async startup of all tables in `databases` after specified jobs `startup_after`.
    /// Note that for every table startup an extra dependency on that table loading will be added along with `startup_after`.
    /// Must be called only after `loadTablesAsync()`.
    [[nodiscard]] LoadTaskPtrs startupTablesAsync(LoadJobSet startup_after = {});

private:
    ContextMutablePtr global_context;
    Databases databases;
    LoadingStrictnessLevel strictness_mode;

    Strings databases_to_load;
    ParsedTablesMetadata metadata;
    TablesDependencyGraph referential_dependencies;
    TablesDependencyGraph loading_dependencies;
    TablesDependencyGraph all_loading_dependencies;
    LoggerPtr log;
    std::atomic<size_t> tables_processed{0};
    AtomicStopwatch stopwatch;

    AsyncLoader & async_loader;
    std::unordered_map<String, LoadTaskPtr> load_table; /// table_id -> load task

    void buildDependencyGraph();
    void removeUnresolvableDependencies();
};

}
