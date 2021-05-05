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
#include <type_traits>
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

static inline std::string getNameFromPath(const std::string& path)
{
    return path.substr(path.rfind('/') + 1);
}

static inline auto valueOr(auto& container, auto key,
    std::remove_cvref_t<decltype(container.at(key))> default_value)
{
    if (auto it = container.find(key); it != container.end())
        return it->second;
    return default_value;
}

static inline void setOrIncrement(auto& container, auto key,
    std::remove_cvref_t<decltype(container.at(key))> value)
{
    if (auto it = container.find(key); it != container.end())
        it->second += value;
    else
        container[key] = value;
}


using EdgeIndex = uint32_t;
using Addr = void *;

using CallCount = size_t;
using Line = size_t;

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
        symbolizeInstrumentedData();
    }

    inline void hit(EdgeIndex edge_index)
    {
        if (copying_test_hits.load())
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

    using AtomicCounter = std::atomic<CallCount>;
    using EdgesHit = std::vector<std::unique_ptr<AtomicCounter>>;
    using EdgesHashmap = std::unordered_map<EdgeIndex, CallCount>;

    EdgesHit edges_hit;
    std::optional<std::string> test;
    std::atomic_bool copying_test_hits = false;
    std::mutex edges_mutex; // protects test, edges_hit

    /// Two caches filled on binary startup from PC table created by clang.
    using EdgesToAddrs = std::vector<Addr>;
    EdgesToAddrs edges_to_addrs;
    std::vector<bool> edge_is_func_entry;

    struct SourceFileInfo
    {
        std::string path;
        std::vector<EdgeIndex> instrumented_functions;
        std::vector<Line> instrumented_lines;

        explicit SourceFileInfo(std::string path_)
            : path(std::move(path_)), instrumented_functions(), instrumented_lines() {}
    };

    /// Two caches filled in symbolizeAllInstrumentedAddrs being read-only by the tests start.
    /// Never cleared.
    std::vector<SourceFileInfo> source_files_cache;
    using SourceFileIndex = decltype(source_files_cache)::size_type;
    std::unordered_map<std::string, SourceFileIndex, StringHash, std::equal_to<>> source_name_to_index;

    struct AddrInfo
    {
        Line line;
        SourceFileIndex index;
    };

    struct FunctionInfo
    {
        // Although it looks like FunctionInfo : AddrInfo, inheritance bring more cons than pros

        Line line;
        SourceFileIndex index;
        std::string_view name;
    };

    /// Two caches filled in symbolizeAllInstrumentedAddrs being read-only by the tests start.
    /// Never cleared.
    std::unordered_map<EdgeIndex, AddrInfo> addr_cache;
    std::unordered_map<EdgeIndex, FunctionInfo> function_cache;

    struct SourceLocation
    {
        std::string full_path;
        Line line;
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

    struct SourceFileData
    {
        std::unordered_map<EdgeIndex, CallCount> functions_hit;
        std::unordered_map<Line, CallCount> lines_hit;
    };

    using TestData = std::vector<SourceFileData>; // vector index = source_files_cache index

    // TODO TestData global_report;

    void prepareDataAndDump(TestInfo test_info, const EdgesHashmap& hits);
    void convertToLCOVAndDump(TestInfo test_info, const TestData& test_data);

    /// Fills addr_cache, function_cache, source_files_cache, source_file_name_to_path_index
    void symbolizeInstrumentedData();

    /// Symbolized data
    struct AddrSym
    {
        /**
         * Multiple callbacks can be called for a single line, e.g. ternary operator
         * a = foo ? bar() : baz() may produce two callbacks as it's basically an if-else block.
         * However, we track _lines_ coverage, not _addresses_ coverage, so we should hash the local cache items by
         * their unique lines.
         *
         * N.B. It's possible to track per-address coverage, LCov's .info format refers to it as _branch_ coverage.
         */
        using Container = std::unordered_map<Line, AddrSym>;

        EdgeIndex index; // will likely be optimized so as AddrSym ~ EdgeIndex.
    };

    struct FuncSym
    {
        /**
         * Template instantiations get mangled into different names, e.g.
         * FN:151,_ZNK2DB7DecimalIiE9convertToIlEET_v
         * FNDA:0,_ZNK2DB7DecimalIiE9convertToIlEET_v
         * FN:151,_ZNK2DB7DecimalIlE9convertToIlEET_v
         * FNDA:0,_ZNK2DB7DecimalIlE9convertToIlEET_v
         * FN:151,_ZNK2DB7DecimalInE9convertToIlEET_v
         * FNDA:0,_ZNK2DB7DecimalInE9convertToIlEET_v
         *
         * We can't use predicate "all functions with same line are considered same" as it's easily
         * contradicted -- multiple functions can be invoked in the same source line, e.g. foo(bar(baz)):
         *
         * FN:166,_ZN2DB7DecimalInEmLERKn
         * FNDA:0,_ZN2DB7DecimalInEmLERKn
         * FN:166,_ZN2DB7DecimalIN4wide7integerILm256EiEEEmLERKS3_
         * FNDA:0,_ZN2DB7DecimalIN4wide7integerILm256EiEEEmLERKS3_
         *
         * However, instantiations are located in different binary parts, so it's safe to use edge_index
         * as a hashing key (current solution is to just use a vector to avoid hash calculation).
         *
         * As a bonus, functions report view in HTML will show distinct counts for each instantiated template.
         */
        using Container = std::vector<FuncSym>;

        Line line;
        EdgeIndex index;
        std::string_view name;

        FuncSym(Line line_, EdgeIndex index_, std::string_view name_): line(line_), index(index_), name(name_) {}
    };

    template <class CacheItem>
    struct LocalCacheItem
    {
       std::string full_path;
       typename CacheItem::Container items;
    };

    template <class CacheItem>
    using LocalCache = std::unordered_map<std::string/*source name*/, LocalCacheItem<CacheItem>>;

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
                    SourceLocation source = getSourceLocation(edge_index);

                    /**
                     * Getting source name as a string (reverse search + allocation) is a bit cheaper than hashing
                     * the full path.
                     */
                    std::string source_name = getNameFromPath(source.full_path);
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
                            cache_it->second.items[source.line] = {edge_index};
                        else
                            cache[std::move(source_name)] =
                                {std::move(source.full_path), {{source.line, {edge_index}}}};
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
    void mergeDataToCaches(const LocalCachesArray<CacheItem>& data)
    {
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
                    for (auto [line, addr_sym] : addrs_data)
                    {
                        info.instrumented_lines.push_back(line);
                        addr_cache[addr_sym.index] = {line, source_index};
                    }
                }
            }

        LOG_INFO(base_log, "Finished merging {} data to caches", thread_name<is_func_cache>);
    }
};
}
