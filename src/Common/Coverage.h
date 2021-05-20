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

#if defined(__ELF__) && !defined(__FreeBSD__)
    #define NON_ELF_BUILD 0
    #include <Common/SymbolIndex.h>
    #include <Common/Dwarf.h>

using SymbolIndex = DB::SymbolIndex;
using SymbolIndexInstance = decltype(SymbolIndex::instance());
using Dwarf = DB::Dwarf;

#else
    /// FreeBSD and Darwin do not have DWARF, so coverage build is explicitly disabled.
    /// Fake classes are introduced to be able to build CH.

    #define NON_ELF_BUILD 1
    #if WITH_COVERAGE
        #error "Coverage build does not work on FreeBSD and Darwin".
    #endif

struct SymbolIndexInstance
{
    struct SymbolPtr { std::string_view name; };
    static constexpr SymbolPtr sym_ptr;

    struct Ptr { constexpr const SymbolPtr * findSymbol(void*) const { return &sym_ptr; } }; //NOLINT
    static constexpr Ptr ptr;

    constexpr const Ptr * operator->() const { return &ptr; }
};

struct SymbolIndex { static constexpr SymbolIndexInstance instance() { return {}; } };

struct Dwarf
{
    struct File { std::string toString() const { return {}; } }; //NOLINT
    struct FileAndLine { size_t line; File file; };

    constexpr FileAndLine findAddressForCoverageRuntime(uintptr_t) const { return {}; } //NOLINT
};

#endif

namespace detail
{
using EdgeIndex = uint32_t;
using Addr = void *;
using Line = int;

using Threads = std::vector<std::thread>;

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
    template <class J>
    inline void schedule(J && job)
    {
        {
            std::lock_guard lock(mutex);
            tasks.emplace(std::forward<J>(job));
        }

        task_or_shutdown.notify_one();
    }

    void start();
    void wait();

    ~TaskQueue() { wait(); }

private:
    using Task = std::function<void()>;

    std::thread worker;
    std::queue<Task> tasks;

    std::mutex mutex;
    std::condition_variable task_or_shutdown;

    bool shutdown {false};
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

    void onClientInitialized();

    void hit(EdgeIndex index);

    void setTestsCount(size_t tests_count);

    void onChangedTestName(std::string old_test_name); // String is passed by value as it's swapped with _test_.

private:
    Writer();

    /// If you want to test this runtime outside of Docker, change these two variables.
    static constexpr std::string_view docker_clickhouse_src_dir_abs_path = "/build/src";
    static constexpr std::string_view docker_report_path = "/report.ccr";

    static constexpr std::string_view logger_base_name = "Coverage";

    static constexpr size_t total_source_files_hint {5000}; // each LocalCache pre-allocates this / pool_size.

    const size_t hardware_concurrency { std::thread::hardware_concurrency() };
    const Poco::Logger * base_log {nullptr}; // Initialized when server initializes Poco internal structures.

    const SymbolIndexInstance symbol_index;
    const Dwarf dwarf;

    const std::filesystem::path clickhouse_src_dir_abs_path { docker_clickhouse_src_dir_abs_path };
    const std::string report_path { docker_report_path };

    // CH client is usually located inside main CH binary, but we don't need to instrument client code.
    // This variable is set on client initialization so we can ignore coverage for it.
    bool is_client {false};

    size_t functions_count {0}; // Initialized after processing PC table.
    size_t addrs_count {0};
    size_t edges_count {0};

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
            std::vector<EdgeIndex> functions_hit;
            std::vector<Line> lines_hit;
        };

        std::string name; // If is empty, there is no data for test.
        size_t test_index;
        const Poco::Logger * log;
        std::vector<SourceData> data; // [i] ~ info for source_files_cache[i].
    };

    /// [i] ~ is edge with index i hit or not.
    /// Can't use vector<bool> as it's not contiguous.
    using EdgesHit = std::vector<char>;

    std::vector<Addr> edges_to_addrs;
    std::vector<bool> edge_is_func_entry;

    std::vector<SourceFileInfo> source_files_cache;
    std::unordered_map<SourceRelativePath, SourceFileIndex> source_rel_path_to_index;

    std::vector<EdgeInfo> edges_cache;

    std::vector<TestData> tests; /// Data accumulated for all tests.
    EdgesHit edges_hit; /// Data accumulated for a single test.

    size_t test_index {0}; /// Index for test that will run next.

    class FileWrapper
    {
        FILE * handle {nullptr};
    public:
        inline void set(const std::string& pathname, const char * mode) { handle = fopen(pathname.data(), mode); }
        inline FILE * file() { return handle; }
        inline void close() { fclose(handle); handle = nullptr; }
        inline void write(const fmt::memory_buffer& mb) { fwrite(mb.data(), 1, mb.size(), handle); }
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

    /// Spawn workers symbolizing addresses obtained from PC table to internal local caches.
    /// Returns the thread pool.
    template <bool is_func_cache, class CacheItem>
    Threads scheduleSymbolizationJobs(LocalCaches<CacheItem>& local_caches, const std::vector<EdgeIndex>& data);

    /// Merge local caches' symbolized data [obtained from multiple threads] into global caches.
    template<bool is_func_cache, class CacheItem>
    void mergeDataToCaches(const LocalCaches<CacheItem>& data);
};
}
