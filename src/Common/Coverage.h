#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>
#include <unordered_map>

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

    inline void hit(void * addr)
    {
        auto lck = std::lock_guard(edges_mutex);
        edges.push_back(addr);
    }

    inline void dump() { dumpAndChangeTestName({}); }

private:
    Writer();

    const std::filesystem::path coverage_dir;

    const MultiVersion<SymbolIndex>::Version symbol_index;
    const Dwarf dwarf;
    const uintptr_t binary_virtual_offset; // TODO Always 0, get rid of.

    FreeThreadPool pool;

    std::optional<std::string> test;
    using Hits = std::vector<void*>;
    Hits edges;
    std::mutex edges_mutex; // protects test, edges

    void dumpAndChangeTestName(std::string_view test_name);

    using SymbolMangledName = std::string;
    struct SymbolData { std::string demangled_name; UInt64 start_line; };
    using SymbolsCache = std::unordered_map<SymbolMangledName, SymbolData>;

    struct AddrInfo
    {
        std::string file;
        UInt64 line;
        const SymbolData& symbol_data;
    };

    using SourceFileName = std::string;

    using FunctionName = std::string;
    struct FunctionData { size_t start_line; size_t call_count; };

    using BranchLine = size_t;
    struct BranchData { size_t block_number; size_t branch_number; size_t taken; };

    using Line = size_t;
    using LineCalled = size_t;

    struct SourceFileData
    {
        std::string full_path;
        std::unordered_map<FunctionName, FunctionData> functions;
        //std::unordered_map<BranchLine, BranchData> branches; //won't fill as for now
        std::unordered_map<Line, LineCalled> lines; // which triggered the callback
    };

    using SourceFiles = std::unordered_map<SourceFileName, SourceFileData>;

    AddrInfo symbolizeAndDemangle(SymbolsCache& symbols_cache, const void * virtual_addr) const;

    void prepareDataAndDumpToDisk(const Hits& hits, std::string_view test_name);

    void convertToLCOVAndDumpToDisk(
        size_t processed_edges, const SourceFiles& source_files, std::string_view test_name);
};
}
