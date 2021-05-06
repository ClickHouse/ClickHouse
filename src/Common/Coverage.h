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
#include <unordered_set>
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
    static constexpr std::string_view coverage_test_name_setting_name = "coverage_test_name";

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
        /**
         * If we are not copying data: it's like multithreaded value increment -- will probably get less hits, but
         * it's still good approximation.
         *
         * If we are copying data: the only place where edges_hit is used is pointer swap with a temporary vector.
         * In -O3 this code will surely be optimized to something like https://godbolt.org/z/8rerbshzT,
         * where only lines 32-36 swap the pointers, so we'll just probably count some new hits for old test.
         */
        ++edges_hit[edge_index];
    }

    /// String is passed by value as it's swapped with _test_.
    void dumpAndChangeTestName(std::string old_test_name);

private:
    Writer();

    static constexpr std::string_view logger_base_name = "Coverage";
    static constexpr std::string_view coverage_dir_relative_path = "../../coverage";
    static constexpr std::string_view coverage_global_report_name = "global_report";

    /**
     * We use all threads (including hyper-threading) for symbolization as server does nothing else.
     * For test processing, we divide the pool size by 2 as some test spawn many threads.
     */
    const size_t hardware_concurrency;

    size_t functions_count;
    size_t addrs_count;

    ///Basically, a sum of two above == edges_hit.size() == edges_to_addrs.size() == edge_is_func_entry.size()
    size_t edges_count;

    const Poco::Logger * base_log; /// do not use the logger before call of serverHasInitialized.

    const std::filesystem::path coverage_dir;

    const MultiVersion<SymbolIndex>::Version symbol_index;
    const Dwarf dwarf;

    FreeThreadPool pool;

    using EdgesHit = std::vector<EdgeIndex>;

    EdgesHit edges_hit;
    std::string test;

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

    struct EdgeInfo
    {
        Line line;
        SourceFileIndex index;
        std::string_view name;

        constexpr bool isFunctionEntry() const { return !name.empty(); }
    };

    /// Cache filled in symbolizeAllInstrumentedAddrs being read-only by the tests start, never cleared.
    /// vector index = edge_index.
    std::vector<EdgeInfo> edges_cache;

    struct SourceLocation
    {
        std::string full_path;
        Line line;
    };

    inline SourceLocation getSourceLocation(EdgeIndex index) const
    {
        /// This binary gets loaded first, so binary_virtual_offset = 0
        const Dwarf::LocationInfo loc = dwarf.findAddressForCoverageRuntime(uintptr_t(edges_to_addrs[index]));
        return {loc.file.toString(), loc.line};
    }

    inline std::string_view symbolize(EdgeIndex index) const
    {
        return symbol_index->findSymbol(edges_to_addrs[index])->name;
    }

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

    TestData global_report;

    void prepareDataAndDump(TestInfo test_info, const EdgesHit& hits);
    void convertToLCOVAndDump(TestInfo test_info, const TestData& test_data);

    /// Fills addr_cache, function_cache, source_files_cache, source_file_name_to_path_index
    void symbolizeInstrumentedData();

    /// Symbolized data
    struct AddrSym
    {
        /**
         * Multiple callbacks can be called for a single line, e.g. ternary operator
         * a = foo ? bar() : baz() may produce two callbacks as it's basically an if-else block.
         * We track _lines_ coverage, not _addresses_ coverage, so we can hash local cache items by their unique lines.
         * However, test may register both of the callbacks, so we need to process all edge indices per line.
         */
        using Container = std::unordered_multimap<Line, EdgeIndex>;
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

    template <class CacheItem>
    using LocalCaches = std::vector<LocalCache<CacheItem>>;

    template <bool is_func_cache>
    static constexpr const std::string_view thread_name = is_func_cache
        ? "func"
        : "addr";

    /// each LocalCache pre-allocates this / pool_size for a small speedup;
    static constexpr size_t total_source_files_hint = 3000;

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
    void scheduleSymbolizationJobs(LocalCaches<CacheItem>& local_caches, const std::vector<EdgeIndex>& data)
    {
        const size_t pool_size = hardware_concurrency;
        const size_t step = data.size() / pool_size;

        for (size_t thread_index = 0; thread_index < pool_size; ++thread_index)
            pool.scheduleOrThrowOnError([this, &local_caches, &data, thread_index, step, pool_size]
            {
                const size_t start_index = thread_index * step;
                const size_t end_index = std::min(start_index + step, data.size() - 1);
                const size_t count = end_index - start_index;

                const Poco::Logger * const log = &Poco::Logger::get(
                    fmt::format("{}.{}{}", logger_base_name, thread_name<is_func_cache>, thread_index));

                LocalCache<CacheItem>& cache = local_caches[thread_index];
                cache.reserve(total_source_files_hint / pool_size);

                for (size_t i = start_index; i < end_index; ++i)
                {
                    const EdgeIndex edge_index = data[i];
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
                            cache_it->second.items.emplace(source.line, edge_index);
                        else
                            cache[std::move(source_name)] =
                                {std::move(source.full_path), {{source.line, edge_index}}};
                    }

                    if (i % 128 == 0)
                        LOG_DEBUG(log, "{}/{}", i - start_index, count);
                }
            });
    }

    template <bool is_func_cache>
    static inline auto& getInstrumented(SourceFileInfo& info)
    {
        if constexpr (is_func_cache)
            return info.instrumented_functions;
        else
            return info.instrumented_lines;
    }

    /// Merge local caches' symbolized data [obtained from multiple threads] into global caches.
    template<bool is_func_cache, class CacheItem>
    void mergeDataToCaches(const LocalCaches<CacheItem>& data)
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
                auto& instrumented = getInstrumented<is_func_cache>(info);

                instrumented.reserve(addrs_data.size());

                if constexpr (is_func_cache)
                    for (auto [line, edge_index, name] : addrs_data)
                    {
                        instrumented.push_back(edge_index);
                        edges_cache[edge_index] = {line, source_index, name};
                    }
                else
                {
                    std::unordered_set<Line> lines;

                    for (auto [line, edge_index] : addrs_data)
                    {
                        if (!lines.contains(line))
                            instrumented.push_back(line);

                        edges_cache[edge_index] = {line, source_index, {}};
                        lines.insert(line);
                    }
                }
            }

        LOG_INFO(base_log, "Finished merging {} data to caches", thread_name<is_func_cache>);
    }
};
}
