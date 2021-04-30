#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>
#include <unordered_map>
#include <Poco/Logger.h>

#include <common/types.h>

#include <Common/SymbolIndex.h>
#include <Common/Dwarf.h>
#include <Common/ThreadPool.h>

namespace detail
{
using namespace DB;

class Writer
{
public:
    static inline Writer& instance()
    {
        static Writer w;
        return w;
    }

    void initialized(uint32_t count);
    void hit(void * addr);

    inline void dump() { dumpAndChangeTestName({}); }

private:
    static constexpr const char * logger_name = "coverage";

    /// How many tests are converted to LCOV in parallel.
    static constexpr const size_t test_processing_thread_pool_size = 4;

    /// How many addresses do we dump into local storage before acquiring the edges_mutex and pushing into edges.
    static constexpr const size_t hits_batch_array_size = 10000;

    /// How many addresses are processed while filling internal data structures in prepareDataAndDumpToDisk before
    /// dumping them to shared cache.
    //static constexpr const size_t hits_batch_processing_size = 1000;

    static constexpr bool test_use_batch = true;

    Writer();

    Poco::Logger * const log;

    const std::filesystem::path coverage_dir;

    const MultiVersion<SymbolIndex>::Version symbol_index;
    const Dwarf dwarf;
    static constexpr const uintptr_t binary_virtual_offset {0}; // As our binary gets loaded first

    FreeThreadPool pool;

    /// How many addresses are currently in the local storage.
    static thread_local inline size_t hits_batch_index = 0;

    static thread_local inline std::array<void*, hits_batch_array_size> hits_batch_storage{};

    std::optional<std::string> test;
    using Hits = std::vector<void*>;
    Hits edges;
    std::mutex edges_mutex; // protects test, edges

    void dumpAndChangeTestName(std::string_view test_name);

    using SourceFileName = std::string;
    using Line = size_t;
    using LineCalled = size_t;

    struct SourceFileData
    {
        std::string full_path;
        //std::unordered_map<FunctionName, FunctionData> functions;
        //std::unordered_map<BranchLine, BranchData> branches;
        std::unordered_map<Line, LineCalled> lines; // which triggered the callback
    };

    using SourceFiles = std::unordered_map<SourceFileName, SourceFileData>;

    using SymbolMangledName = std::string;
    using SymbolStartLine = size_t;
    using SymbolsCache = std::unordered_map<SymbolMangledName, SymbolStartLine>;

    struct AddrInfo
    {
        SourceFileData& file;
        //SymbolStartLine symbol_start_line;
        UInt64 line;
    };

    //using FunctionName = std::string;
    //struct FunctionData { size_t start_line; size_t call_count; };

    //using BranchLine = size_t;
    //struct BranchData { size_t block_number; size_t branch_number; size_t taken; };

    AddrInfo symbolize(SourceFiles& files, SymbolsCache& symbols_cache, const void * virtual_addr) const;

    void prepareDataAndDumpToDisk(const Hits& hits, std::string_view test_name);

    void convertToLCOVAndDumpToDisk(
        size_t processed_edges, const SourceFiles& source_files, std::string_view test_name);
};
}
