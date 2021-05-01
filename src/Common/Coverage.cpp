#include "Coverage.h"

#include <algorithm>
#include <cassert>

#include <fstream>
#include <iterator>
#include <optional>
#include <string>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "Common/ProfileEvents.h"
#include "common/logger_useful.h"
#include <common/demangle.h>

#include <Interpreters/Context.h>
#include <Poco/Logger.h>


namespace detail
{
namespace
{
inline auto getInstanceAndInitGlobalCounters()
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
}

Writer::Writer()
    : base_log(&Poco::Logger::get(std::string{logger_base_name})),
      coverage_dir(std::filesystem::current_path() / Writer::coverage_dir_relative_path),
      symbol_index(getInstanceAndInitGlobalCounters()),
      dwarf(symbol_index->getSelf()->elf),
      // 0 -- unlimited queue e.g. functor insertion to thread pool won't lock.
      pool(Writer::thread_pool_size, 1000, 0)
{
    Context::setSettingHook("coverage_test_name", [this](const Field& value)
    {
        const std::string& name = value.get<String>();
        dumpAndChangeTestName(name);
    });

    if (std::filesystem::exists(coverage_dir))
    {
        size_t suffix = 1;
        const std::string dir_path = coverage_dir.string();

        while (std::filesystem::exists(dir_path + "_" + std::to_string(suffix)))
            ++suffix;

        const std::string dir_new_path = dir_path + "_" + std::to_string(suffix);
        std::filesystem::rename(coverage_dir, dir_new_path);

        LOG_INFO(base_log, "Found previous run directory, moved it to {}", dir_new_path);
    }

    std::filesystem::create_directory(coverage_dir);
}

void Writer::initializedGuards(uint32_t count)
{
    edges.reserve(count);
    LOG_INFO(base_log, "Initialized guards, {} instrumented edges total", count);
}


void Writer::initializePCTable(const uintptr_t *pcs_beg, const uintptr_t *pcs_end)
{
    for (const auto *it = pcs_beg; it != pcs_end; ++it)
    {
        const uintptr_t pair = *it;
        void * const addr = reinterpret_cast<void *>(intptr_t(pair >> 1));
        const bool is_function_entry = pair & 1;

        if (is_function_entry)
            initializeFunctionEntry(addr);
    }

    LOG_INFO(base_log, "Initialized PC table, {} functions, {} non-function edges",
        function_cache.size(), edges.size() - function_cache.size());
}


void Writer::initializeFunctionEntry(void * addr)
{

}

void Writer::hit(void * addr)
{
    if constexpr (test_use_batch)
    {
        if (hits_batch_index == hits_batch_array_size - 1) //non-atomic, ok as thread_local.
        {
            auto lck = std::lock_guard(edges_mutex);

            if (test)
            {
                hits_batch_storage[hits_batch_index] = addr; //can insert last element;
                edges.insert(edges.end(), hits_batch_storage.begin(), hits_batch_storage.end());
            }

            hits_batch_index = 0;

            return;
        }

        hits_batch_storage[hits_batch_index++] = addr;
    }
    else
    {
        auto lck = std::lock_guard(edges_mutex);

        if (test)
            edges.push_back(addr);
    }
}

void Writer::dumpAndChangeTestName(std::string_view test_name)
{
    std::string old_test_name;
    Hits edges_copies;
    const Poco::Logger * log {nullptr};

    {
        auto lck = std::lock_guard(edges_mutex);

        if (!test)
        {
            test = test_name;
            return;
        }

        log = &Poco::Logger::get(std::string{logger_base_name} + *test);
        LOG_INFO(log, "Started copying data", test_name);

        if constexpr (test_use_batch)
            if (hits_batch_index > 0) // haven't copied last addresses from local storage to edges
            {
                edges.insert(edges.end(),
                    hits_batch_storage.begin(), std::next(hits_batch_storage.begin(), hits_batch_index));

                hits_batch_index = 0;
            }

        edges_copies = edges; //TODO Check if it's fast
        old_test_name = *test;

        if (test_name.empty())
            test = std::nullopt;
        else
        {
            test = test_name;
            edges.clear();
        }
    }

    LOG_INFO(log, "Copied shared data");

    /// Can't copy by ref as current function's lifetime may end before evaluating the functor.
    /// The move is evaluated within current function's lifetime during function constructor call.
    auto f = [this, log, test_name = std::move(old_test_name), edges_copied = std::move(edges_copies)]
    {
        prepareDataAndDump(log, edges_copied, test_name);
    };

    // The functor insertion itself is thread-safe.
    pool.scheduleOrThrowOnError(std::move(f));
    LOG_INFO(log, "Scheduled job");
}

Writer::AddrInfo Writer::symbolize(
    SourceFiles& files,
    SymbolsCache& //symbols_cache
    , const void * virtual_addr) const
{
    const uintptr_t physical_addr = uintptr_t(virtual_addr) - binary_virtual_offset;
    const Dwarf::LocationInfo loc = dwarf.findAddressForCoverageRuntime(physical_addr);

    std::string file_name_path = loc.file.toString();
    std::string file_name = file_name_path.substr(file_name_path.rfind('/') + 1);

    SourceFiles::iterator file_data_it = files.find(file_name);

    if (file_data_it == files.end())
        file_data_it = files.emplace(
            std::move(file_name),
            SourceFileData{.full_path = std::move(file_name_path)}).first;

    //const auto * symbol = symbol_index->findSymbol(virtual_addr);
    //
    //SymbolsCache::iterator symbol_it = symbols_cache.find(symbol->name);

    //if (symbol_it == symbols_cache.end())
    //{
    //    const void * const symbol_start_virtual = symbol->address_begin;
    //    const uintptr_t symbol_start_phys = uintptr_t(symbol_start_virtual) - binary_virtual_offset;
    //    const UInt64 symbol_start_line = dwarf.findAddressForCoverageRuntime(symbol_start_phys).line;

    //    symbol_it = symbols_cache.emplace(symbol->name, symbol_start_line).first;
    //}

    return {file_data_it->second, loc.line};
}

void Writer::prepareDataAndDump(const Poco::Logger * log, const Writer::Hits& hits, std::string_view test_name)
{
    LOG_INFO(log, "Started filling internal structures, {} hits", hits.size());

    SourceFiles source_files;

    using AddrsCache = std::unordered_map<void*, AddrInfo>;
    AddrsCache addrs_cache;

    SymbolsCache symbols_cache;

    time_t elapsed = time(nullptr);

    for (size_t i = 0; i < hits.size(); ++i)
    {
        void * const addr = hits.at(i);

        if (auto it = function_cache.find(addr); it != function_cache.end())
        {

        }

        AddrsCache::iterator iter = addrs_cache.find(addr);

        if (iter == addrs_cache.end())
            iter = addrs_cache.emplace(addr, symbolize(source_files, symbols_cache, addr)).first;

        const AddrInfo& addr_info = iter->second;

        if (const time_t current = time(nullptr); current > elapsed)
        {
            LOG_INFO(log, "Processed {}/{}", i, hits.size());
            elapsed = current;
        }

        SourceFileData& data = addr_info.file;

        if (auto it = data.lines_hit.find(addr_info.line); it == data.lines_hit.end())
            data.lines_hit[addr_info.line] = 1;
        else
            ++it->second;

        // const SymbolData& symbol_data = addr_info.symbol_data;
        // if (auto it = data.functions.find(symbol_data.demangled_name); it == data.functions.end())
        //     data.functions[symbol_data.demangled_name] = {symbol_data.start_line, .call_count = 0};
        // else
        //     ++it->second.call_count;
    }

    LOG_INFO(log, "Finished filling internal structures");
    convertToLCOVAndDump(log, source_files, test_name);
}

void Writer::convertToLCOVAndDump(
    const Poco::Logger * log, const Writer::SourceFiles& source_files, std::string_view test_name)
{
    /**
     * [incomplete] LCOV .info format reference, parsed from
     * https://github.com/linux-test-project/lcov/blob/master/bin/geninfo
     *
     * Tests description .td file:
     *
     * TN:<test name>
     * TD:<test description>
     *
     * Report .info file:
     *
     * TN:<test name>
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
     *         // note -- there's third parameter "line contents", but looks like it's useless
     *         DA:<line number>,<execution count>
     *
     *     LF:<number of lines instrumented (found)>
     *     LH:<number of lines executed (hit)>
     *     end_of_record
     */
    LOG_INFO(log, "Started dumping");

    std::ofstream ofs(coverage_dir / test_name);

    fmt::print(ofs, "TN:{}\n", test_name);

    for (const auto& [name, data] : source_files)
    {
        fmt::print(ofs, "SF:{}\n", data.full_path);

        for (const auto & [addr, call_count]: data.functions_hit)
        {
            const auto& [func_name, start_line] = function_cache.find(addr)->second;
            fmt::print(ofs, "FN:{0},{1}\nFNDA:{2},{1}\n", start_line, func_name, call_count);
        }

        fmt::print(ofs, "FNF:{}\nFNH:{}\n", function_cache.size(), data.functions_hit.size());

        // TODO Branches

        for (auto [line, call_count] : data.lines_hit)
            fmt::print(ofs, "DA:{},{}\n", line, call_count);

        fmt::print(ofs, "LF:{}\nLH:{}\nend_of_record\n", data.lines_hit.size(), data.lines_hit.size());
    }

    LOG_INFO(log, "Finished dumping");
    Poco::Logger::destroy(log->name());
}
}
