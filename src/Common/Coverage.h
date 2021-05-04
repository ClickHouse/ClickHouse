#pragma once

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <functional>
#include <iterator>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <Poco/Logger.h>

#include "common/logger_useful.h"
#include <common/types.h>

#include <Common/SymbolIndex.h>
#include <Common/Dwarf.h>
#include <Common/ThreadPool.h>

namespace detail
{
using namespace DB;

/// To be able to search by std::string_view in std::string key container
struct StringHash
{
    using hash_type = std::hash<std::string_view>;
    using is_transparent = void;

    size_t operator()(std::string_view str) const   { return hash_type{}(str); }
    size_t operator()(std::string const& str) const { return hash_type{}(str); }
};

static inline std::string_view getNameFromPath(std::string_view path)
{
    return path.substr(path.rfind('/') + 1);
}

// These two functions should have distinct names not to allow implicit conversions.
static inline std::string getNameFromPathStr(const std::string& path)
{
    return path.substr(path.rfind('/') + 1);
}

static inline auto valueOr(auto& container, auto key, decltype(container.at(key)) default_value)
{
    if (auto it = container.find(key); it != container.end())
        return it->second;
    return default_value;
}


using EdgeIndex = uint32_t;
using Addr = void *;

struct SourceFileData
{
    std::unordered_map<EdgeIndex, size_t /* call count */> functions_hit;
    std::unordered_map<size_t /* line */, size_t /* call_count */> lines_hit;
};

struct SourceFileInfo
{
    std::string path;
    std::vector<EdgeIndex> instrumented_functions;
    std::vector<size_t> instrumented_lines;

    explicit SourceFileInfo(std::string path_)
        : path(std::move(path_)), instrumented_functions(), instrumented_lines() {}
};

class Writer
{
public:
    static inline Writer& instance()
    {
        static Writer w;
        return w;
    }

    void initializePCTable(const uintptr_t *pc_array, const uintptr_t *pc_array_end);

    inline void serverHasInitialized()
    {
        /// Before server has initialized, we can't log data to Poco.
        base_log = &Poco::Logger::get(std::string{logger_base_name});
        loadCacheOrSymbolizeInstrumentedData();
    }

    inline void hit(EdgeIndex edge_index)
    {
        if (dumping.load())
        {
            auto lck = std::lock_guard(edges_mutex);
            edges_hit.at(edge_index)->fetch_add(1);
        }
        else
            edges_hit.at(edge_index)->fetch_add(1);
    }

private:
    Writer();

    static constexpr std::string_view logger_base_name = "Coverage";
    static constexpr std::string_view coverage_dir_relative_path = "../../coverage";
    //static constexpr std::string_view coverage_cache_file_path = "../../symbolized.cache";

    /// How many tests are converted to LCOV in parallel.
    static constexpr size_t thread_pool_test_processing = 10;

    /// How may threads concurrently symbolize the addresses on binary startup if cache was not present.
    static constexpr size_t thread_pool_symbolizing = 16;

    size_t functions_count;
    size_t addrs_count;

    const Poco::Logger * base_log; /// do not use the logger before call of serverHasInitialized.

    const std::filesystem::path coverage_dir;

    const MultiVersion<SymbolIndex>::Version symbol_index;
    const Dwarf dwarf;

    FreeThreadPool pool;

    using EdgesHit = std::vector<std::unique_ptr<std::atomic_size_t>>;
    using EdgesHashmap = std::unordered_map<EdgeIndex, size_t>;

    EdgesHit edges_hit;
    std::optional<std::string> test;
    std::atomic_bool dumping = false;
    std::mutex edges_mutex; // protects test, edges_hit

    /// Two caches filled on binary startup from PC table created by clang.
    using EdgesToAddrs = std::vector<Addr>;
    EdgesToAddrs edges_to_addrs;
    std::vector<bool> edge_is_func_entry;

    /// Four caches filled in symbolizeAllInstrumentedAddrs being read-only by the tests start.
    /// Never cleared.
    using SourceFileIndex = size_t;
    std::unordered_map<std::string, SourceFileIndex, StringHash, std::equal_to<>> source_name_to_index;
    std::vector<SourceFileInfo> source_files_cache;

    struct AddrInfo
    {
        size_t line;
        SourceFileIndex index;
    };

    struct FunctionInfo // Although it looks like FunctionInfo : AddrInfo, inheritance bring more cons than pros
    {
        size_t line;
        SourceFileIndex index;
        std::string_view name;
    };

    std::unordered_map<EdgeIndex, AddrInfo> addr_cache;
    std::unordered_map<EdgeIndex, FunctionInfo> function_cache;

    struct SourceLocation
    {
        std::string full_path;
        size_t line;
    };

    inline SourceLocation getSourceLocation(EdgeIndex index) const
    {
        /// This binary gets loaded first, so binary_virtual_offset = 0
        const Dwarf::LocationInfo loc = dwarf.findAddressForCoverageRuntime(uintptr_t(edges_to_addrs.at(index)));
        return {loc.file.toString(), loc.line};
    }

    inline std::string_view symbolize(EdgeIndex index) const
    {
        return symbol_index->findSymbol(edges_to_addrs.at(index))->name;
    }

    void dumpAndChangeTestName(std::string_view test_name);

    struct TestInfo
    {
        std::string_view name;
        const Poco::Logger * log;
    };

    using TestData = std::vector<SourceFileData>; // vector index = source_files_cache index

    void prepareDataAndDump(TestInfo test_info, const EdgesHashmap& hits);
    void convertToLCOVAndDump(TestInfo test_info, const TestData& test_data);

    /// Fills addr_cache, function_cache, source_files_cache, source_file_name_to_path_index
    void loadCacheOrSymbolizeInstrumentedData();

    /// Symbolized data
    struct AddrSym
    {
        size_t line;
        EdgeIndex index;
        AddrSym(size_t line_, EdgeIndex index_): line(line_), index(index_) {}
    };

    struct FuncSym // Although it looks like FuncSym : AddrSym, inheritance bring more cons than pros
    {
        size_t line;
        EdgeIndex index;
        std::string_view name;
        FuncSym(size_t line_, EdgeIndex index_, std::string_view name_): line(line_), index(index_), name(name_) {}
    };

    template <class CacheItem>
    struct LocalCacheItem
    {
       std::string full_path;
       std::vector<CacheItem> items;
    };

    template <class CacheItem>
    using LocalCache = std::unordered_map<std::string/* source name*/, LocalCacheItem<CacheItem>>;

    template <class CacheItem, size_t ArraySize = thread_pool_symbolizing>
    using LocalCachesArray = std::array<LocalCache<CacheItem>, ArraySize>;

    template <bool is_func_cache>
    static constexpr const std::string_view thread_name = is_func_cache
        ? "func"
        : "addr";

    /**
     * Spawn workers symbolizing addresses obtained from PC table to internal local caches.
     *
     * TODO
     * Unlike function caches, addresses symbolization jobs tend to work nonuniformly.
     * Usually, all but one jobs finish, and the last eats up about 1 extra minute.
     * The idea is to spawn 2x jobs with same thread pool size, so the chance most threads will be idle is lower.
     * The drawback here is that all source files locations must be recalculated in each thread, so it will
     * take some extra time.
     */
    template <bool is_func_cache, class CacheItem>
    void scheduleSymbolizationJobs(LocalCachesArray<CacheItem>& local_caches, const std::vector<EdgeIndex>& data)
    {
        constexpr auto pool_size = thread_pool_symbolizing;

        const size_t step = data.size() / pool_size;

        for (size_t thread_index = 0; thread_index < pool_size; ++thread_index)
            pool.scheduleOrThrowOnError([this, &local_caches, &data, thread_index, step]
            {
                const size_t start_index = thread_index * step;
                const size_t end_index = std::min(start_index + step, data.size() - 1);

                const Poco::Logger * const log = &Poco::Logger::get(
                    fmt::format("{}.{}{}", logger_base_name, thread_name<is_func_cache>, thread_index));

                LocalCache<CacheItem>& cache = local_caches[thread_index];

                time_t elapsed = time(nullptr);

                for (size_t i = start_index; i < end_index; ++i)
                {
                    const EdgeIndex edge_index = data.at(i);
                    SourceLocation source = getSourceLocation(edge_index); //non-const so we could move .full_path

                    /**
                     * Getting source name as a string (reverse search + allocation) is a bit cheaper than hashing
                     * the full path.
                     */
                    std::string source_name = getNameFromPathStr(source.full_path); //non-const so we could move it
                    auto cache_it = cache.find(source_name);

                    if constexpr (is_func_cache)
                    {
                        const std::string_view name = symbolize(edge_index);

                        if (cache_it != cache.end())
                            cache_it->second.items.emplace_back(source.line, edge_index, name);
                        else
                            cache[std::move(source_name)] =
                                {std::move(source.full_path), {{source.line, edge_index, name}}};
                    }
                    else
                    {
                        if (cache_it != cache.end())
                            cache_it->second.items.emplace_back(source.line, edge_index);
                        else
                            cache[std::move(source_name)] =
                                {std::move(source.full_path), {{source.line, edge_index}}};
                    }

                    if (time_t current = time(nullptr); current > elapsed)
                    {
                        LOG_DEBUG(log, "{}/{}", i - start_index, end_index - start_index);
                        elapsed = current;
                    }
                }
            });
    }

    /// Merge local caches' symbolized data [obtained from multiple threads] into global caches.
    template<bool is_func_cache, class CacheItem>
    void mergeDataToCaches(const LocalCachesArray<CacheItem>& data) {
        LOG_INFO(base_log, "Started merging {} data to caches", thread_name<is_func_cache>);

        for (const auto& cache : data)
            for (const auto& [source_name, path_and_addrs] : cache)
            {
                const auto& [source_path, addrs_data] = path_and_addrs;

                SourceFileIndex source_index;

                if (auto it = source_name_to_index.find(source_name); it != source_name_to_index.end())
                    source_index = it->second;
                else
                {
                    source_index = source_name_to_index.size();
                    source_name_to_index.emplace(source_name, source_index);
                    source_files_cache.emplace_back(source_path);
                }

                SourceFileInfo& info = source_files_cache[source_index];

                if constexpr (is_func_cache)
                {
                    for (auto [line, edge_index, name] : addrs_data)
                    {
                        info.instrumented_functions.push_back(edge_index);
                        function_cache[edge_index] = {line, source_index, name};
                    }
                }
                else
                {
                    for (auto [line, edge_index] : addrs_data)
                    {
                        info.instrumented_lines.push_back(line);
                        addr_cache[edge_index] = {line, source_index};
                    }
                }
            }

        LOG_INFO(base_log, "Finished merging {} data to caches", thread_name<is_func_cache>);
    }
};
}
