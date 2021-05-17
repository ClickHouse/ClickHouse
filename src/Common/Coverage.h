#pragma once

#include <cstdio>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <functional>
#include <iterator>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <queue>
#include <list>
#include <string_view>
#include <type_traits>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include <fmt/format.h>
#include <Poco/Logger.h>

#include "common/logger_useful.h"
#include <common/types.h>

#include <Common/SymbolIndex.h>
#include <Common/Dwarf.h>

namespace detail
{
using namespace DB;

using EdgeIndex = uint32_t;
using Addr = void *;

using CallCount = size_t;
using Line = size_t;

/**
 * Simplified FreeThreadPool from Common/ThreadPool.h . Uses one external thread.
 * Not intended for general use (some invariants broken).
 *
 * - Does not throw.
 * - Does not use metrics.
 * - Some operations are not thread-safe.
 *
 * Own implementation needed as Writer does not use most FreeThreadPool features (and we can save ~20 minutes by
 * using this class instead of the former).
 *
 * Each test duration is longer than coverage test processing pipeline (converting and dumping to disk), so no
 * more than 1 thread is needed.
 */
class TaskQueue
{
public:
    TaskQueue(): worker([this] { workerThread(); }) {} //NOLINT

    template <class J>
    inline void schedule(J && job)
    {
        {
            std::lock_guard lock(mutex);
            tasks.emplace(std::forward<J>(job));
        }

        task_or_shutdown.notify_one();
    }

    ~TaskQueue();

private:
    using Task = std::function<void()>;

    std::thread worker;
    std::queue<Task> tasks;

    std::mutex mutex;
    std::condition_variable task_or_shutdown;

    bool shutdown = false;

    void workerThread();
};

/**
 * Custom code coverage runtime for -WITH_COVERAGE=1 build.
 *
 * On startup, symbolizes all instrumented addresses in binary (information provided by clang).
 * During testing, handles calls from sanitizer callbacks (Common/CoverageCallbacks.h).
 * Writes a report in CCR format (docs/development/coverage.md) after testing.
 */
class Writer
{
public:
    static constexpr std::string_view setting_test_name = "coverage_test_name";
    static constexpr std::string_view setting_tests_count = "coverage_tests_count";

    static Writer& instance();

    void initializeRuntime(const uintptr_t *pc_array, const uintptr_t *pc_array_end);

    void onServerInitialized();

    void hit(EdgeIndex edge_index);

    inline void setTestsCount(size_t tests_count)
    {
        // This is an upper bound (as we don't know which tests will be skipped before running them).
        tests.resize(tests_count);
    }

    void onChangedTestName(std::string old_test_name); // String is passed by value as it's swapped with _test_.

private:
    Writer();

    static constexpr std::string_view logger_base_name = "Coverage";

    static constexpr size_t total_source_files_hint = 5000; // each LocalCache pre-allocates this / pool_size.

    const size_t hardware_concurrency;
    const Poco::Logger * base_log;

    const MultiVersion<SymbolIndex>::Version symbol_index;
    const Dwarf dwarf;

    const std::filesystem::path clickhouse_src_dir_abs_path;
    const std::string report_path;

    size_t functions_count;
    size_t addrs_count;
    size_t edges_count;

    TaskQueue tasks_queue;

    using SourceRelativePath = std::string; // from ClickHouse src/ directory

    struct SourceFileInfo
    {
        SourceRelativePath relative_path;
        std::vector<EdgeIndex> instrumented_functions;
        std::vector<Line> instrumented_lines;

        explicit SourceFileInfo(std::string path_)
            : relative_path(std::move(path_)), instrumented_functions(), instrumented_lines() {}
    };

    using SourceFileIndex = size_t;

    struct EdgeInfo
    {
        Line line;
        SourceFileIndex index;
        std::string_view name;

        constexpr bool isFunctionEntry() const { return !name.empty(); }
    };

    struct TestData
    {
        struct SourceData
        {
            std::unordered_map<EdgeIndex, CallCount> functions_hit;
            std::unordered_map<Line, CallCount> lines_hit;
        };

        std::string name; // If is empty, there is no data for test.
        size_t test_index;
        const Poco::Logger * log;
        std::vector<SourceData> data; // [i] means source file in source_files_cache with index i.
    };

    using EdgesHit = std::vector<EdgeIndex>;

    std::vector<Addr> edges_to_addrs;
    std::vector<bool> edge_is_func_entry;

    std::vector<SourceFileInfo> source_files_cache;
    std::unordered_map<SourceRelativePath, SourceFileIndex> source_rel_path_to_index;

    std::vector<EdgeInfo> edges_cache;

    std::vector<TestData> tests; /// Data accumulated for all tests.
    EdgesHit edges_hit; /// Data accumulated for a single test.

    size_t test_index;

    class FileWrapper /// RAII wrapper for FILE *
    {
        FILE * handle {nullptr};
    public:
        inline void set(const std::string& pathname, const char * mode) { handle = fopen(pathname.data(), mode); }
        inline FILE * file() { return handle; }
        inline void close() { fclose(handle); handle = nullptr; }
        inline void write(const fmt::memory_buffer& mb) { fwrite(mb.data(), mb.size(), sizeof(char), handle); }
    };

    FileWrapper report_file;

    void deinitRuntime();

    void prepareDataAndDump(TestData& test_data, const EdgesHit& hits);

    // CCR (ClickHouse coverage report) is a format for storing large coverage reports while preserving per-test data.

    void writeCCRHeader();
    void writeCCREntry(const TestData& test_data);
    void writeCCRFooter();

    // Symbolization

    void symbolizeInstrumentedData();

    template <class CacheItem>
    using LocalCache = std::unordered_map<SourceRelativePath, typename CacheItem::Container>;

    template <class CacheItem>
    using LocalCaches = std::vector<LocalCache<CacheItem>>;

public:
    using Threads = std::vector<std::thread>;
private:

    /// Spawn workers symbolizing addresses obtained from PC table to internal local caches.
    /// Returns the thread pool.
    template <bool is_func_cache, class CacheItem>
    Threads scheduleSymbolizationJobs(LocalCaches<CacheItem>& local_caches, const std::vector<EdgeIndex>& data);

    /// Merge local caches' symbolized data [obtained from multiple threads] into global caches.
    template<bool is_func_cache, class CacheItem>
    void mergeDataToCaches(const LocalCaches<CacheItem>& data);
};
}
