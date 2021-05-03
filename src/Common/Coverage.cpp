#include "Coverage.h"

#include <algorithm>

#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "Common/ProfileEvents.h"

#include <Interpreters/Context.h>

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
    : base_log(nullptr),
      coverage_dir(std::filesystem::current_path() / Writer::coverage_dir_relative_path),
      symbol_index(getInstanceAndInitGlobalCounters()),
      dwarf(symbol_index->getSelf()->elf),
      // 0 -- unlimited queue e.g. functor insertion to thread pool won't lock.
      pool(Writer::thread_pool_test_processing, 1000, 0)
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
    }

    std::filesystem::create_directory(coverage_dir);
}

void Writer::initializePCTable(const uintptr_t *pc_array, const uintptr_t *pc_array_end)
{
    const size_t edges_pairs = pc_array_end - pc_array;
    const size_t edges = edges_pairs / 2;

    edges_hit.reserve(edges);

    edges_to_addrs.resize(edges);
    edge_is_func_entry.resize(edges);

    for (size_t i = 0; i < edges_pairs; i += 2)
    {
        edges_to_addrs[i / 2] = reinterpret_cast<void *>(pc_array[i]);
        edge_is_func_entry[i / 2] = pc_array[i + 1] & 1;
    }

    /// We don't symbolize the addresses right away, wait for CH application to load instead.
    /// If starting now, we won't be able to log to Poco or std::cout;
}

void Writer::symbolizeAllInstrumentedAddrs()
{
    LOG_INFO(base_log, "Started symbolizing addresses");

    std::vector<EdgeIndex> function_indices;
    std::vector<EdgeIndex> addr_indices;

    function_indices.reserve(edges_to_addrs.size());
    addr_indices.reserve(edges_to_addrs.size());

    for (size_t i = 0; i < edges_to_addrs.size(); ++i)
        if (edge_is_func_entry.at(i))
            function_indices.push_back(i);
        else
            addr_indices.push_back(i);

    // TODO Debug only.
    for (size_t i = 0; i < 10; ++i)
    {
        auto a = getSourceLocation(function_indices.at(i));
        LOG_INFO(base_log, "Func: {} {}", a.full_path, a.line);

        const Dwarf::LocationInfo loc = dwarf.findAddressForCoverageRuntime(
            uintptr_t(edges_to_addrs.at(addr_indices.at(i))) - 5);
        LOG_INFO(base_log, "Addr: {} {}", loc.file.toString(), loc.line);
    }

    pool.setMaxThreads(thread_pool_symbolizing);

    LocalCachesArray<FuncSym> func_caches{};
    scheduleSymbolizationJobs<true>(func_caches, function_indices);

    pool.wait();

    LOG_INFO(base_log, "Symbolized all functions");

    LocalCachesArray<AddrSym> addr_caches{};

    scheduleSymbolizationJobs<false>(addr_caches, addr_indices);

    /// Merge functions data while other threads process addresses.
    mergeDataToCaches<true>(func_caches);

    pool.wait();

    mergeDataToCaches<false>(addr_caches);

    pool.setMaxThreads(thread_pool_test_processing);

    LOG_INFO(base_log, "Symbolized all addresses");
}

void Writer::dumpAndChangeTestName(std::string_view test_name)
{
    std::string old_test_name;
    EdgesHit edges_hit_copy;

    const Poco::Logger * log {nullptr};

    {
        auto lck = std::lock_guard(edges_mutex);

        if (!test)
        {
            test = test_name;
            return;
        }

        log = &Poco::Logger::get(std::string{logger_base_name} + "." + *test);
        LOG_INFO(log, "Started moving data");

        edges_hit_copy = std::move(edges_hit);
        old_test_name = *test;

        if (test_name.empty())
            test = std::nullopt;
        else
        {
            test = test_name;
            edges_hit = {}; // to initialize after move.
        }
    }

    LOG_INFO(log, "Moved shared data, {} addrs", edges_hit_copy.size());

    /// Can't copy by ref as current function's lifetime may end before evaluating the functor.
    /// The move is evaluated within current function's lifetime during function constructor call.
    auto f = [this, log, test_name = std::move(old_test_name), edges_copied = std::move(edges_hit_copy)]
    {
        prepareDataAndDump({test_name, log}, edges_copied);
    };

    // The functor insertion itself is thread-safe.
    pool.scheduleOrThrowOnError(std::move(f));
}

void Writer::prepareDataAndDump(TestInfo test_info, const EdgesHit& hits)
{
    LOG_INFO(test_info.log, "Started filling internal structures, {} addrs", hits.size());

    TestData test_data(source_files_cache.size());

    time_t elapsed = time(nullptr);
    size_t i = 0;

    for (auto [edge_index, hit] : hits)
    {
        ++i;

        if (const time_t current = time(nullptr); current > elapsed)
        {
            LOG_DEBUG(test_info.log, "Processed {}/{}", i, hits.size());
            elapsed = current;
        }

        if (auto it = function_cache.find(edge_index); it != function_cache.end())
        {
            auto& functions = test_data[it->second.index].functions_hit;

            if (auto it2 = functions.find(edge_index); it2 == functions.end())
                functions[edge_index] = hit;
            else
                it2->second += hit;

            continue;
        }

        // lines_hit is broken.

        if (addr_cache.find(edge_index) == addr_cache.end())
        {
            LOG_ERROR(test_info.log, "Fault edge index {}", edge_index);
            continue;
        }

        const AddrInfo& addr_cache_entry = addr_cache.at(edge_index);
        auto& lines = test_data[addr_cache_entry.index].lines_hit;

        if (auto it = lines.find(addr_cache_entry.line); it == lines.end())
            lines[addr_cache_entry.line] = hit;
        else
            it->second += hit;
    }

    LOG_INFO(test_info.log, "Finished filling internal structures");
    convertToLCOVAndDump(test_info, test_data);
}

void Writer::convertToLCOVAndDump(TestInfo test_info, const TestData& test_data)
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
    LOG_INFO(test_info.log, "Started dumping");

    std::ofstream ofs(coverage_dir / test_info.name);

    fmt::print(ofs, "TN:{}\n", test_info.name);

    for (size_t i = 0; i < test_data.size(); ++i)
    {
        const auto& [functions_hit, lines_hit] = test_data.at(i);
        const auto& [path, functions_instrumented, lines_instrumented] = source_files_cache.at(i);

        fmt::print(ofs, "SF:{}\n", path);

        for (EdgeIndex index : functions_instrumented)
        {
            const FunctionInfo& func_info = function_cache.at(index);
            const size_t call_count = valueOr(functions_hit, index, 0);

            fmt::print(ofs, "FN:{0},{1}\nFNDA:{2},{1}\n", func_info.line, func_info.name, call_count);
        }

        fmt::print(ofs, "FNF:{}\nFNH:{}\n", functions_instrumented.size(), functions_hit.size());

        for (size_t line : lines_instrumented)
        {
            const size_t call_count = valueOr(lines_hit, line, 0);
            fmt::print(ofs, "DA:{},{}\n", line, call_count);
        }

        fmt::print(ofs, "LF:{}\nLH:{}\nend_of_record\n", lines_instrumented.size(), lines_hit.size());
    }

    LOG_INFO(test_info.log, "Finished dumping");
    Poco::Logger::destroy(test_info.log->name());
}
}
