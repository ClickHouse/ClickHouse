#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <Interpreters/Context_fwd.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

struct SharedContextHolder;

/**
 * Storage Memory Profiler Tool
 *
 * A lightweight ClickHouse binary for profiling memory consumption of storages,
 * tables, and other server components. Based on LocalServer but with jemalloc
 * heap profiling capabilities.
 *
 * Usage:
 *   clickhouse-examples storage_memory_profiler -f 01.sql -f 02.sql -f 03.sql --output-dir profiles
 *
 * Features:
 *   - Full query execution (CREATE TABLE, INSERT, SELECT, etc.)
 *   - Heap dumps between SQL files using jemalloc profiler
 *   - Optional auto-symbolization of heap dumps
 *   - Minimal background noise (no system log tables)
 */
class StorageMemoryProfiler
{
public:
    ~StorageMemoryProfiler();

    int run(const VectorWithMemoryTracking<String> & args);

private:
    void setupLogging();
    void initializeContext();
    void registerComponents();
    void setupDatabase();
    void cleanup();

    /// Execute all queries from a SQL file
    void executeQueriesFromFile(const std::string & filepath);

    /// jemalloc profiling helpers
    void flushJemallocThreadCache();
    void refreshJemallocEpoch();
    size_t getJemallocAllocated();
    bool isProfilingEnabled();
    bool isProfilingCompiled();
    std::string dumpProfile(const std::string & label);

    /// Print usage information
    void printUsage();
    void printProfilingStatus();

    /// Configuration
    VectorWithMemoryTracking<String> sql_files;
    std::string output_dir = ".";
    std::string profile_prefix = "memory_profile_";
    std::string data_path;
    bool no_system_tables = false;
    bool symbolize = false;

    /// Server context
    std::unique_ptr<SharedContextHolder> shared_context;
    ContextMutablePtr global_context;
    ContextMutablePtr session_context;
    std::optional<std::filesystem::path> temporary_directory_to_delete;
};

}
