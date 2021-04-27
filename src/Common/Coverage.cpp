#include "Coverage.h"

#include <cassert>

#include <fstream>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <common/demangle.h>

#include <Interpreters/Context.h>


namespace detail
{

Writer::Writer()
    : coverage_dir(std::filesystem::current_path() / "../../coverage"),
      symbol_index(SymbolIndex::instance()),
      dwarf(symbol_index->getSelf()->elf),
      binary_virtual_offset(uintptr_t(symbol_index->getSelf()->address_begin)),
      pool(4)
{
    Context::setSettingHook("coverage_test_name", [this](const Field& value)
    {
        dump();
        auto lck = std::lock_guard(edges_mutex);
        test = value.get<std::string>();
    });
}

void Writer::dump()
{
    {
        auto lck = std::lock_guard(edges_mutex);
        if (!test)
            return;
    }

    pool.scheduleOrThrowOnError([this] () mutable // TODO Maybe should protect with mutex?
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

Writer::AddrInfo Writer::symbolizeAndDemangle(const void * virtual_addr) const
{
    AddrInfo out =
    {
        .virtual_addr = virtual_addr,
        .physical_addr = reinterpret_cast<void *>(uintptr_t(virtual_addr) - binary_virtual_offset)
    };

    const Dwarf::LocationInfo loc = dwarf.findAddressForCoverageRuntime(uintptr_t(out.physical_addr));
    out.file = loc.file.toString();
    out.line = loc.line;

    const auto * symbol = symbol_index->findSymbol(out.virtual_addr);
    // won't cache addresses as there are lots of them.
    assert(symbol);

    // TODO check if better with demangler/start_line cache
    //
    int status = 0;
    out.symbol = demangle(symbol->name, status);
    assert(!status);
    //
    const void * symbol_start_virtual = symbol->address_begin;
    const uintptr_t symbol_start_phys = uintptr_t(symbol_start_virtual) - binary_virtual_offset;
    out.symbol_start_line = dwarf.findAddressForCoverageRuntime(symbol_start_phys).line;

    return out;
}

void Writer::prepareDataAndDumpToDisk(const Writer::Hits& hits, std::string_view test_name)
{
    SourceFiles source_files;
    std::unordered_map<void*, AddrInfo> addrs_cache;
    //std::unordered_map<SymbolMangledName, SymbolData> symbol_cache;

    for (void * addr : hits)
    {
        AddrInfo addr_info;

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
void Writer::convertToLCOVAndDumpToDisk(const Writer::SourceFiles& source_files, std::string_view test_name)
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
}
