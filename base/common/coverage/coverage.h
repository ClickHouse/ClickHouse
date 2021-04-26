#pragma once

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <filesystem>
#include <unordered_map>
#include <queue>
#include <vector>
#include <shared_mutex>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "common/types.h"
#include "common/demangle.h"

#include <Common/Dwarf.h>
#include <Common/Elf.h>
#include <Common/SymbolIndex.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>

namespace detail
{
using namespace DB;

class Writer
{
public:
    static Writer& instance()
    {
        static Writer w;
        return w;
    }

    void initialized(uint32_t count, const uint32_t * start)
    {
        (void)start;

        Context::setSettingHook("coverage_test_name", [this](const Field& value)
        {
            dump();
            auto lck = std::lock_guard(edges_mutex);
            test = value.get<std::string>();
        });

        std::filesystem::remove_all(coverage_dir);
        std::filesystem::create_directory(coverage_dir);

        edges.reserve(count);
    }

    void hit(uint32_t edge_index, void * addr)
    {
        (void)edge_index;
        auto lck = std::lock_guard(edges_mutex);
        edges.push_back(addr);
    }

    void dump()
    {
        if (!test)
            return;

        pool.scheduleOrThrowOnError([this] () mutable
        {
            Hits edges_copies;
            std::string test_name;

            {
                auto lock = std::lock_guard(edges_mutex);
                edges_copies = edges;
                test_name = *test;
                test = std::nullopt; //already copied the data, can process the new test
                edges.clear(); // hope that it's O(1).
            }

            prepareDataAndDumpToDisk(edges_copies, test_name);
        });
    }

private:
    Writer()
        : coverage_dir(std::filesystem::current_path() / "../../coverage"),
          symbol_index(SymbolIndex::instance()),
          pool(4)
    {
        const SymbolIndex & sym_index = *symbol_index;
        const auto * object = sym_index.getSelf();

        binary_virtual_offset = object->address_begin;
        dwarf = object->elf; //create wrapper
    }

    const std::filesystem::path coverage_dir;
    const uintptr_t binary_virtual_offset;

    const MultiVersion<SymbolIndex>::Version symbol_index;
    const Dwarf dwarf;

    FreeThreadPool pool;

    std::optional<std::string> test;
    using Hits = std::vector<void*>;
    Hits edges;
    std::mutex edges_mutex; // to prevent multithreading inserts

    struct Frame
    {
        const void * virtual_addr;
        void * physical_addr;
        std::string symbol; // function
        std::string file;
        UInt64 line;
        UInt64 symbol_start_line;
    };

    Frame symbolizeAndDemangle(const void * virtual_addr) const
    {
        Frame out =
        {
            .virtual_addr = virtual_addr,
            .physical_addr = reinterpret_cast<void *>(uintptr_t(virtual_addr) - binary_virtual_offset)
        };

        const Dwarf::LocationInfo loc = dwarf.findAddressForCoverageRuntime(uintptr_t(out.physical_addr));
        out.file = loc.file.toString();
        out.line = loc.line;

        const auto * symbol = symbol_index.findSymbol(out.virtual_addr);
        assert(symbol);

        int status = 0;
        out.symbol = demangle(symbol->name, status);
        assert(!status);

        // TODO should cache, no need to recalc for each line.
        const void * symbol_start_virtual = symbol->address_begin;
        const uintptr_t symbol_start_phys = uintptr_t(symbol_start_virtual) - binary_virtual_offset;
        out.symbol_start_line = dwarf.findAddressForCoverageRuntime(symbol_start_phys).line;

        return out;
    }

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

        // Not all of the instrumented lines in all basic blocks, but only those lines which triggered the callback.
        std::unordered_map<Line, LineCalled> lines;
    };

    using SourceFiles = std::unordered_map<SourceFileName, SourceFileData>;
    using AddrsCache = std::unordered_map<void*, Frame>;

    void prepareDataAndDumpToDisk(const Hits& hits, std::string_view test_name)
    {
        SourceFiles source_files;
        AddrsCache addrs_cache;

        for (void * addr : hits)
        {
            Frame addr_info;

            if (auto it = addrs_cache.find(addr); it != addrs_cache.end())
                addr_info = it->second;
            else
            {
                addr_info = symbolizeAndDemangle(addr);
                addrs_cache.emplace(addr, addr_info);
            }

            const std::string file_name = addr_info.file.substr(addr_info.file.rfind('/'));

            SourceFiles::iterator file_data_it = source_files.find(file_name);

            if (file_data_it == source_files.end())
                file_data_it = source_files.emplace(file_name, SourceFileData{.full_path=addr_info.file}).first;

            SourceFileData& data = file_data_it->second;

            if (auto it = data.lines.find(addr_info.line); it == data.lines.end())
                data.lines[addr_info.line] = 1;
            else
                ++it->second;

            if (auto it = data.functions.find(addr_info.symbol); it == data.functions.end())
                data.functions[addr_info.symbol] = {.start_line = addr_info.symbol_start_line, .call_count = 0};
            else
                ++it->second.call_count;
        }

        convertToLCOVAndDumpToDisk(source_files, test_name);
    }

    /**
     * [incomplete] LCOV .info format reference, parsed from
     * https://github.com/linux-test-project/lcov/blob/master/bin/geninfo
     *
     * TN:<test name>
     * for each source file:
     *     SF:<absolute path to the source file>
     *
     *     for each instrumented function:
     *         FN:<line number of function start>,<function name>
     *         FNDA:<call-count>, <function-name>
     *
     *     if >0 functions instrumented:
     *         FNF:<number of functions instrumented (found)>
     *         FNH:<number of functions executed (hit)>
     *
     *     for each instrumented branch:
     *         BRDA:<line number>,<block number>,<branch number>,<taken -- number > 0 or "-" if was not taken>
     *
     *     if >0 branches instrumented:
     *         BRF:<number of branches instrumented (found)>
     *         BRH:<number of branches executed (hit)>
     *
     *     for each instrumented line:
     *         DA:<line number>,<execution count>
     *
     *     LF:<number of lines instrumented (found)>
     *     LH:<number of lines executed (hit)>
     *     end_of_record
     */
    void convertToLCOVAndDumpToDisk(const SourceFiles& source_files, std::string_view test_name)
    {
        std::ofstream ofs(coverage_dir / test_name);

        fmt::print(ofs, "TN:{}\n", test_name);

        for (const auto& [name, data] : source_files)
        {
            fmt::print(ofs, "SF:{}\n", data.full_path);

            size_t fnh = 0;

            for (const auto & [func_name, func_data]: data.functions)
            {
                fmt::print(ofs, "FN:{0},{1}\nFNDA:{2},{1}\n",
                        func_data.start_line,
                        func_name,
                        func_data.call_count);

                if (func_data.call_count > 0)
                    ++fnh;
            }

            assert(!data.functions.empty());

            fmt::print(ofs, "FNF:{}\nFNH:{}\n", data.functions.size(), fnh);

            // TODO Branches

            size_t lh = 0;

            for (auto [line, calls] : data.lines)
            {
                fmt::print(ofs, "DA:{},{}\n", line, calls);

                if (calls > 0)
                    ++lh;
            }

            fmt::print(ofs, "LF:{}\nLH:{}\nend_of_record\n", data.lines.size(), lh);
        }
    }
};
}
