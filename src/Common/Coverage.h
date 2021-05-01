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

    /// Called when guard variables for all instrumented edges have been initialized.
    void initializedGuards(uint32_t count);

    /// Called when class needs to store all instrumented addresses.
    void initializePCTable(const uintptr_t *pcs_beg, const uintptr_t *pcs_end);

    /// Called when a critical edge in binary is hit.
    void hit(void * addr);

private:
    static constexpr const std::string_view logger_base_name = "Coverage";
    static constexpr const std::string_view coverage_dir_relative_path = "../../coverage";

    /// How many tests are converted to LCOV in parallel.
    static constexpr const size_t thread_pool_size = 6;

    /// How many addresses do we dump into local storage before acquiring the edges_mutex and pushing into edges.
    static constexpr const size_t hits_batch_array_size = 10000;

    /// How many addresses are processed while filling internal data structures in prepareDataAndDumpToDisk before
    /// dumping them to shared cache.
    //static constexpr const size_t hits_batch_processing_size = 1000;

    static constexpr bool test_use_batch = true;

    Writer();

    const Poco::Logger * const base_log;

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

    using FunctionAddr = void *;
    using FunctionCallCount = size_t;

    using SourceFileName = std::string;
    using Line = size_t;
    using LineCalled = size_t;

    struct SourceFileData
    {
        std::string full_path;
        // TODO Branches
        std::unordered_map<FunctionAddr, FunctionCallCount> functions_hit;
        std::unordered_map<Line, LineCalled> lines_hit;
    };

    using SourceFiles = std::unordered_map<SourceFileName, SourceFileData>;

    using SymbolMangledName = std::string;
    using SymbolStartLine = size_t;
    using SymbolsCache = std::unordered_map<SymbolMangledName, SymbolStartLine>;

    struct AddrInfo
    {
        SourceFileData& file;
        size_t line;
    };

    struct FunctionData
    {
        std::string_view name;
        size_t start_line;
        std::string source_file_name; //TODO Think about avoiding allocations
    };

    /// CH has a relatively small number of functions, so we can symbolize all of them before starting the tests.
    /// No mutex needed as by the tests start it will be readonly.
    std::unordered_map<FunctionAddr, FunctionData> function_cache;

    void initializeFunctionEntry(void * addr);

    //using BranchLine = size_t;
    //struct BranchData { size_t block_number; size_t branch_number; size_t taken; };

    AddrInfo symbolize(SourceFiles& files, SymbolsCache& symbols_cache, const void * virtual_addr) const;

    void prepareDataAndDump(const Poco::Logger * log, const Hits& hits, std::string_view test_name);
    void convertToLCOVAndDump(const Poco::Logger * log, const SourceFiles& source_files, std::string_view test_name);
};
}
