#pragma once

#include <atomic>
#include <filesystem>
#include <functional>
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

using Addr = void *;
using Addrs = std::vector<Addr>;

struct SourceFileData
{
    std::unordered_map<Addr, size_t /* call count */> functions_hit;
    std::unordered_map<size_t /* line */, size_t /* call_count */> lines_hit;
};

struct SourceFileInfo
{
    std::string path;
    std::vector<Addr> instrumented_functions;
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

    /// Called when class needs to store all instrumented addresses.
    void initializePCTable(const uintptr_t *pcs_beg, const uintptr_t *pcs_end);

    /// Called when guard variables for all instrumented edges have been initialized.
    inline void initializedGuards(uint32_t count) { edges.reserve(count); }

    /// Before server has initialized, we can't log data to Poco.
    inline void serverHasInitialized()
    {
        base_log = &Poco::Logger::get(std::string{logger_base_name});
        symbolizeAllInstrumentedAddrs();
    }

    /// Called when a critical edge in binary is hit.
    void hit(void * addr);

private:
    Writer();

    static constexpr const std::string_view logger_base_name = "Coverage";
    static constexpr const std::string_view coverage_dir_relative_path = "../../coverage";

    /// How many tests are converted to LCOV in parallel.
    static constexpr const size_t thread_pool_test_processing = 10;

    /// How may threads concurrently symbolize the addresses on binary startup.
    static constexpr const size_t thread_pool_symbolizing = 16;

    /// How many addresses do we dump into local storage before acquiring the edges_mutex and pushing into edges.
    static constexpr const size_t hits_batch_array_size = 100000;

    static thread_local inline size_t hits_batch_index = 0; /// How many addresses are currently in the local storage.
    static thread_local inline std::array<void*, hits_batch_array_size> hits_batch_storage{};

    const Poco::Logger * base_log; /// do not use the logger before call of serverHasInitialized.

    const std::filesystem::path coverage_dir;

    const MultiVersion<SymbolIndex>::Version symbol_index;
    const Dwarf dwarf;

    FreeThreadPool pool;

    Addrs edges;
    std::optional<std::string> test;
    std::mutex edges_mutex; // protects test, edges

    /// Two caches filled on binary startup from PC table created by clang.
    /// Cleared after addresses symbolization in symbolizeAllInstrumentedAddrs.
    Addrs pc_table_addrs;
    Addrs pc_table_function_entries;

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

    struct FunctionInfo : AddrInfo
    {
        std::string_view name;
    };

    std::unordered_map<Addr, AddrInfo> addr_cache;
    std::unordered_map<Addr, FunctionInfo> function_cache;

    struct SourceLocation
    {
        std::string full_path;
        size_t line;
    };

    inline SourceLocation getSourceLocation(const void * virtual_addr) const
    {
        /// This binary gets loaded first, so binary_virtual_offset = 0
        const Dwarf::LocationInfo loc = dwarf.findAddressForCoverageRuntime(uintptr_t(virtual_addr));
        return {loc.file.toString(), loc.line};
    }

    inline std::string_view symbolize(const void * virtual_addr) const
    {
        return symbol_index->findSymbol(virtual_addr)->name;
    }

    static inline std::string_view getNameFromPath(std::string_view path)
    {
        return path.substr(path.rfind('/') + 1);
    }

    void dumpAndChangeTestName(std::string_view test_name);

    struct TestInfo
    {
        std::string_view name;
        const Poco::Logger * log;
    };

    using TestData = std::vector<SourceFileData>; // vector index = source_files_cache index

    void prepareDataAndDump(TestInfo test_info, const Addrs& addrs);
    void convertToLCOVAndDump(TestInfo test_info, const TestData& test_data);

    /// Fills addr_cache, function_cache, source_files_cache, source_file_name_to_path_index
    /// Clears pc_table_addrs, pc_table_function_entries.
    void symbolizeAllInstrumentedAddrs();

    /// Symbolized data
    struct AddrSym
    {
        size_t line;
        void * addr;

        AddrSym(size_t line_, void * addr_): line(line_), addr(addr_) {}
    };

    struct FuncSym : AddrSym
    {
        std::string_view name;

        FuncSym(size_t line_, void * addr_, std::string_view name_): AddrSym(line_, addr_), name(name_) {}
    };

    template <class CacheItem> using LocalCachesArray = std::array<
        std::unordered_map<std::string/*full_path*/, std::vector<CacheItem>>,
        thread_pool_symbolizing>;

    /**
     * Spawn workers symbolizing addresses obtained from PC table to internal local caches.
     *
     * Unlike func_caches, addresses symbolization jobs tend to work nonuniformly.
     * Usually, all but one jobs finish, and the last eats up about 1 extra minute.
     * The idea is to spawn 2x jobs with same thread pool size, so the chance most threads will be idle is lower.
     * The drawback here is that all source files locations must be recalculated in each thread, so it will
     * take some extra time.
     */
    template <bool is_func_cache, class CacheItem>
    void scheduleSymbolizationJobs(LocalCachesArray<CacheItem>& caches, const Addrs& addrs)
    {
        constexpr auto pool_size = thread_pool_symbolizing;

        constexpr const std::string_view target_str = is_func_cache
            ? "func"
            : "addr";

        const size_t step = addrs.size() / pool_size;
        const auto begin = addrs.cbegin();
        const auto r_end = addrs.cend();

        for (size_t thread_index = 0; thread_index < pool_size; ++thread_index)
            pool.scheduleOrThrowOnError([this, &caches, thread_index, begin, step, r_end, target_str]
            {
                const auto start = begin + thread_index * step;
                const auto end = thread_index == pool_size - 1
                    ? r_end
                    : start + step;

                const Poco::Logger * const log = &Poco::Logger::get(
                    fmt::format("{}.{}{}", logger_base_name, target_str, thread_index));

                const size_t size = end - start;

                auto& cache = caches[thread_index];

                time_t elapsed = time(nullptr);

                for (auto it = start; it != end; ++it)
                {
                    Addr addr = *it;

                    const SourceLocation source = getSourceLocation(addr);

                    if constexpr (is_func_cache)
                    {
                        if (auto cache_it = cache.find(source.full_path); cache_it != cache.end())
                            cache_it->second.emplace_back(source.line, addr, symbolize(addr));
                        else
                            cache[source.full_path] = {{source.line, addr, symbolize(addr)}};
                    }
                    else
                    {
                        if (auto cache_it = cache.find(source.full_path); cache_it != cache.end())
                            cache_it->second.emplace_back(source.line, addr);
                        else
                            cache[source.full_path] = {{source.line, addr}};
                    }

                    if (time_t current = time(nullptr); current > elapsed)
                    {
                        LOG_DEBUG(log, "{}/{}", it - start, size);
                        elapsed = current;
                    }
                }
            });
    }

    /// Merge symbolized data obtained from multiple threads into global caches.
    template<bool is_func_cache, class CacheItem>
    void mergeDataToCaches(const LocalCachesArray<CacheItem>& data, Addrs& to_clear) {
        constexpr const std::string_view target_str = is_func_cache
            ? "function"
            : "addrs";

        LOG_INFO(base_log, "Started merging {} data to caches", target_str);

        for (const auto& cache : data)
            for (const auto& [source_path, addrs_data] : cache)
            {
                const std::string_view source_name = getNameFromPath(source_path);

                SourceFileIndex index;

                if (auto it = source_name_to_index.find(source_name);
                    it != source_name_to_index.end())
                    index = it->second;
                else
                {
                    index = source_name_to_index.size();
                    source_name_to_index.emplace(source_name, index);
                    source_files_cache.emplace_back(source_path);
                }

                SourceFileInfo& info = source_files_cache[index];

                if constexpr (is_func_cache)
                {
                    for (const auto& e: addrs_data) // can't decompose as both base and child have data.
                    {
                        info.instrumented_functions.push_back(e.addr);
                        function_cache[e.addr] = {{e.line, index}, e.name};
                    }
                }
                else
                {
                    for (auto [line, addr] : addrs_data)
                    {
                        info.instrumented_lines.push_back(line);
                        addr_cache[addr] = {line, index};
                    }
                }
            }

        to_clear.clear();
        to_clear.shrink_to_fit();

        LOG_INFO(base_log, "Finished merging {} data to caches", target_str);
    }
};
}
