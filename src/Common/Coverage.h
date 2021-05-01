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
    static constexpr const std::string_view logger_base_name = "Application.Coverage";
    static constexpr const std::string_view coverage_dir_relative_path = "../../coverage";

    /// How many tests are converted to LCOV in parallel.
    static constexpr const size_t thread_pool_size = 8;

    /// How many addresses do we dump into local storage before acquiring the edges_mutex and pushing into edges.
    static constexpr const size_t hits_batch_array_size = 100000;

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

    using Addr = void *;
    using Addrs = std::vector<Addr>;

    Addrs edges;
    std::optional<std::string> test;
    std::mutex edges_mutex; // protects test, edges

    void dumpAndChangeTestName(std::string_view test_name);

    using Line = size_t;

    struct SourceFileData
    {
        using FunctionCallCount = size_t;
        using LineCalled = size_t;

        std::unordered_map<Addr, FunctionCallCount> functions_hit;
        std::unordered_map<Line, LineCalled> lines_hit;
    };

    // vector index = source_file_paths index
    using TestData = std::vector<SourceFileData>;

    using SourceFilePathIndex = size_t;

    struct SourceFileInfo
    {
        std::string path;
        std::vector<Addr> instrumented_functions{};
        std::vector<Line> instrumented_lines{};

        explicit SourceFileInfo(const std::string& path_): path(path_) {}
    };

    struct AddrInfo
    {
        size_t line;
        SourceFilePathIndex index;
    };

    struct FunctionInfo
    {
        std::string_view name;
        size_t start_line;
        SourceFilePathIndex index;
    };

    std::unordered_map<std::string, SourceFilePathIndex> source_file_name_to_path_index;
    std::vector<SourceFileInfo> source_files_cache;

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

    struct TestInfo
    {
        std::string_view name;
        const Poco::Logger * log;
    };

    /// Fill addr_cache, function_cache, source_files_cache, source_file_name_to_path_index
    void symbolizeAllInstrumentedAddrs(const Addrs& function_entries, const Addrs& addrs);

    /// Possibly fill source_files_cache, source_file_name_to_path_index
    std::pair<size_t, size_t> getIndexAndLine(void * addr);

    void prepareDataAndDump(TestInfo test_info, const Addrs& addrs);
    void convertToLCOVAndDump(TestInfo test_info, const TestData& test_data);
};
}
