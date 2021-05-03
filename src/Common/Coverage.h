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

    /// Before server has initialized, we can't log data to Poco.
    inline void serverHasInitialized()
    {
        base_log = &Poco::Logger::get(std::string{logger_base_name});
        symbolizeAllInstrumentedAddrs();
    }

    inline void hit(EdgeIndex edge_index)
    {
        auto lck = std::lock_guard(edges_mutex);

        if (auto it = edges_hit.find(edge_index); it == edges_hit.end())
            edges_hit[edge_index] = 1;
        else
            ++it->second;
    }

private:
    Writer();

    static constexpr const std::string_view logger_base_name = "Coverage";
    static constexpr const std::string_view coverage_dir_relative_path = "../../coverage";

    /// How many tests are converted to LCOV in parallel.
    static constexpr const size_t thread_pool_test_processing = 10;

    /// How may threads concurrently symbolize the addresses on binary startup.
    static constexpr const size_t thread_pool_symbolizing = 16;

    const Poco::Logger * base_log; /// do not use the logger before call of serverHasInitialized.

    const std::filesystem::path coverage_dir;

    const MultiVersion<SymbolIndex>::Version symbol_index;
    const Dwarf dwarf;

    FreeThreadPool pool;

    using EdgesHit = std::unordered_map<EdgeIndex, size_t /* hits */>;

    //static thread_local inline EdgesHit edges_hit_local{};

    EdgesHit edges_hit;
    std::optional<std::string> test;
    std::mutex edges_mutex; // protects test, edges_hit

    /// Two caches filled on binary startup from PC table created by clang.
    /// Never cleared.
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

    static inline std::string_view getNameFromPath(std::string_view path)
    {
        return path.substr(path.rfind('/') + 1);
    }

    static inline auto valueOr(auto& container, auto key, decltype(container.at(key)) default_value)
    {
        if (auto it = container.find(key); it != container.end())
            return it->second;
        return default_value;
    }

    void dumpAndChangeTestName(std::string_view test_name);

    struct TestInfo
    {
        std::string_view name;
        const Poco::Logger * log;
    };

    using TestData = std::vector<SourceFileData>; // vector index = source_files_cache index

    void prepareDataAndDump(TestInfo test_info, const EdgesHit& hits);
    void convertToLCOVAndDump(TestInfo test_info, const TestData& test_data);

    /// Fills addr_cache, function_cache, source_files_cache, source_file_name_to_path_index
    void symbolizeAllInstrumentedAddrs();

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
    using LocalCache = std::unordered_map<std::string, std::vector<CacheItem>>;

    template <class CacheItem, size_t ArraySize = thread_pool_symbolizing>
    using LocalCachesArray = std::array<LocalCache<CacheItem>, ArraySize>;

    /**
     * Spawn workers symbolizing addresses obtained from PC table to internal local caches.
     *
     * Unlike function caches, addresses symbolization jobs tend to work nonuniformly.
     * Usually, all but one jobs finish, and the last eats up about 1 extra minute.
     * The idea is to spawn 2x jobs with same thread pool size, so the chance most threads will be idle is lower.
     * The drawback here is that all source files locations must be recalculated in each thread, so it will
     * take some extra time.
     */
    template <bool is_func_cache, class CacheItem, size_t ArraySize>
    void scheduleSymbolizationJobs(LocalCachesArray<CacheItem, ArraySize>& caches, const std::vector<EdgeIndex>& data)
    {
        constexpr auto pool_size = thread_pool_symbolizing;

        constexpr const std::string_view target_str = is_func_cache
            ? "func"
            : "addr";

        const size_t step = data.size() / pool_size;

        for (size_t thread_index = 0; thread_index < pool_size; ++thread_index)
            pool.scheduleOrThrowOnError([this, &caches, &data, thread_index, step, target_str]
            {
                const size_t start_index = thread_index * step;
                const size_t end_index = std::min(start_index + step, data.size() - 1);

                const Poco::Logger * const log = &Poco::Logger::get(
                    fmt::format("{}.{}{}", logger_base_name, target_str, thread_index));

                LocalCache<CacheItem>& cache = caches[thread_index];

                time_t elapsed = time(nullptr);

                for (size_t i = start_index; i < end_index; ++i)
                {
                    const EdgeIndex edge_index = data.at(i);
                    const SourceLocation source = getSourceLocation(edge_index);

                    // BUG line = 0 for every item in addr_cache
                    // However, line is correct for func_cache, should investigate

                    if constexpr (is_func_cache)
                    {
                        const std::string_view name = symbolize(edge_index);

                        if (auto cache_it = cache.find(source.full_path); cache_it != cache.end())
                            cache_it->second.emplace_back(source.line, edge_index, name);
                        else
                            cache[source.full_path] = {{source.line, edge_index, name}};
                    }
                    else
                    {
                        if (auto cache_it = cache.find(source.full_path); cache_it != cache.end())
                            cache_it->second.emplace_back(source.line, edge_index);
                        else
                            cache[source.full_path] = {{source.line, edge_index}};
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
    template<bool is_func_cache, class CacheItem, size_t ArraySize>
    void mergeDataToCaches(const LocalCachesArray<CacheItem, ArraySize>& data) {
        constexpr const std::string_view target_str = is_func_cache
            ? "func"
            : "addr";

        LOG_INFO(base_log, "Started merging {} data to caches", target_str);

        for (const auto& cache : data)
            for (const auto& [source_path, addrs_data] : cache)
            {
                const std::string_view source_name = getNameFromPath(source_path);

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

        LOG_INFO(base_log, "Finished merging {} data to caches", target_str);
    }
};
}
