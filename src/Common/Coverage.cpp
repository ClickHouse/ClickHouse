#include "Coverage.h"

#include <cassert>

#include <fstream>
#include <optional>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "Common/ProfileEvents.h"
#include <common/demangle.h>

#include <Interpreters/Context.h>


namespace detail
{
static inline auto getInstanceAndInitGlobalCounters()
{
    /**
     * Writer is a singleton, so it initializes statically.
     * SymbolIndex uses a MMapReadBufferFromFile which uses ProfileEvents.
     * If no thread was found in the events profiler, a global variable global_counters is used.
     *
     * This variable may get initialized after Writer (static initialization order fiasco).
     * In fact, the __sanitizer_cov_trace_pc_guard_init is called before the global_counters init.
     *
     * We can't use constinit on that variable as it has a shared_ptr on it, so we just
     * ultimately initialize it before getting the instance.
     *
     * We can't initialize global_counters in ProfileEvents.cpp to nullptr as in that case it will become nullptr.
     * So we just initialize it twice (here and in ProfileEvents.cpp).
     */
    ProfileEvents::global_counters = ProfileEvents::Counters(ProfileEvents::global_counters_array);

    return SymbolIndex::instance();
}

Writer::Writer()
    : coverage_dir(std::filesystem::current_path() / "../../coverage"),
      symbol_index(getInstanceAndInitGlobalCounters()),
      dwarf(symbol_index->getSelf()->elf),
      binary_virtual_offset(uintptr_t(symbol_index->getSelf()->address_begin)),
      pool(4)
{
    Context::setSettingHook("coverage_test_name", [this](const Field& value)
    {
        const std::string& name = value.get<String>();
        dumpAndChangeTestName(name);
    });
}

void Writer::initialized(uint32_t count)
    {
        if (std::filesystem::exists(coverage_dir))
        {
            size_t suffix = 1;
            const std::string dir_path = coverage_dir.string();

            while (std::filesystem::exists(dir_path + "_" + std::to_string(suffix)))
                ++suffix;

            std::filesystem::rename(coverage_dir, dir_path);
        }

        std::filesystem::create_directory(coverage_dir);

        edges.reserve(count);
    }

void Writer::dumpAndChangeTestName(std::string_view test_name)
{
    std::string old_test_name;
    Hits edges_copies;

    {
        auto lck = std::lock_guard(edges_mutex);

        if (!test)
        {
            test = test_name;
            return;
        }

        edges_copies = edges;
        old_test_name = *test;

        if (test_name.empty())
            test = std::nullopt;
        else
        {
            test = test_name;
            edges.clear();
        }
    }

    /// Can't copy by ref as current function's lifetime may end before evaluating the functor.
    /// The move is evaluated within current function's lifetime during function constructor call.
    auto f = [this, test_name = std::move(old_test_name), edges_copied = std::move(edges_copies)]
    {
        prepareDataAndDumpToDisk(edges_copied, test_name);
    };

    // The functor insertion itself is thread-safe.
    pool.scheduleOrThrowOnError(std::move(f));
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

    //for (void * addr : hits)
    for (size_t i = 0; i < hits.size(); ++i)
    {
        std::cout << i << "/" << hits.size() << "\n";

        void * addr = hits.at(i);

        AddrInfo addr_info;

        if (auto it = addrs_cache.find(addr); it != addrs_cache.end())
            addr_info = it->second;
        else
        {
            addr_info = symbolizeAndDemangle(addr);
            addrs_cache.emplace(addr, addr_info);
        }

        const std::string file_name = addr_info.file.substr(addr_info.file.rfind('/') + 1);

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

    convertToLCOVAndDumpToDisk(hits.size(), source_files, test_name);
}

void Writer::convertToLCOVAndDumpToDisk(
    size_t processed_edges, const Writer::SourceFiles& source_files, std::string_view test_name)
{
    /**
     * [incomplete] LCOV .info format reference, parsed from
     * https://github.com/linux-test-project/lcov/blob/master/bin/geninfo
     *
     * TN:<test name>
     * TD:<test description>
     *
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
    std::ofstream ofs(coverage_dir / test_name);

    fmt::print(ofs, "TN:{}\nTD:{} edges\n", test_name, processed_edges);

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
