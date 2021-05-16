#include "Coverage.h"

#include <algorithm>
#include <atomic>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include <fmt/core.h>
#include <fmt/format.h>

#include "Common/ProfileEvents.h"

namespace detail
{
namespace
{
inline decltype(SymbolIndex::instance()) getInstanceAndInitGlobalCounters()
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

template <class T>
inline typename T::mapped_type valueOr(
    const T& container, typename T::key_type key, typename T::mapped_type default_value)
{
    if (auto it = container.find(key); it != container.end())
        return it->second;
    return default_value;
}

template <class T>
inline void setOrIncrement(T& container, typename T::key_type key, typename T::mapped_type value)
{
    if (auto it = container.find(key); it != container.end())
        it->second += value;
    else
        container[key] = value;
}
}

void TaskQueue::worker(size_t thread_id)
{
    while (true)
    {
        Job job;

        {
            std::unique_lock lock(mutex);
            new_job_or_shutdown.wait(lock, [this] { return shutdown || !jobs.empty(); });

            if (jobs.empty())
                return;

            job = std::move(jobs.front());
            jobs.pop();
        }

        job(thread_id);
        job = {};

        {
            std::unique_lock lock(mutex);
            --scheduled_jobs;
        }

        job_finished.notify_all();
    }
}

void TaskQueue::finalize()
{
    {
        std::unique_lock lock(mutex);
        shutdown = true;
    }

    new_job_or_shutdown.notify_all();

    for (auto & thread : threads)
        thread.join();

    threads.clear();
}

Writer::Writer()
    : hardware_concurrency(std::thread::hardware_concurrency()),
      base_log(nullptr),
      // TODO works only in docker
      symbol_index(getInstanceAndInitGlobalCounters()),
      dwarf(symbol_index->getSelf()->elf),
      clickhouse_src_dir_abs_path("/build/src"),
      functions_count(0),
      addrs_count(0),
      edges_count(0),
      // Set the initial pool size to all threads as we'll need all resources to symbolize fast.
      tasks_queue(hardware_concurrency),
      test_index(0),
      ccr_header_buffer() { }

Writer& Writer::instance()
{
    /// Meyer's singleton.
    static Writer w;
    return w;
}

void Writer::initializeRuntime(const uintptr_t *pc_array, const uintptr_t *pc_array_end)
{
    const size_t edges_pairs = pc_array_end - pc_array;
    edges_count = edges_pairs / 2;

    edges_hit.resize(edges_count, 0);
    edges_cache.resize(edges_count);

    edges_to_addrs.resize(edges_count);
    edge_is_func_entry.resize(edges_count);

    for (size_t i = 0; i < edges_pairs; i += 2)
    {
        const bool is_function_entry = pc_array[i + 1] & 1;

        edge_is_func_entry[i / 2] = is_function_entry;

        functions_count += is_function_entry;
        addrs_count += !is_function_entry;

        /**
         * If we use non-function-entry addr as is, the SymbolIndex won't be able to find the line for our address.
         * General assembly looks like this:
         *
         * 0x12dbca75 <+117>: callq  0x12dd2680                ; DB::AggregateFunctionFactory::registerFunction at AggregateFunctionFactory.cpp:39
         * 0x12dbca7a <+122>: jmp    0x12dbca7f                ; <+127> at AggregateFunctionCount.cpp << ADDRESS SHOULD POINT HERE
         * 0x12dbca7f <+127>: movabsq $0x15b4522c, %rax         ; imm = 0x15B4522C << BUT POINTS HERE
         * 0x12dbca89 <+137>: addq   $0x4, %rax
         * 0x12dbca8f <+143>: movq   %rax, %rdi
         * 0x12dbca92 <+146>: callq  0xb067180 ; ::__sanitizer_cov_trace_pc_guard(uint32_t *) at CoverageCallbacks.h:15
         *
         * The symbolizer (as well as lldb and gdb) thinks that instruction at 0x12dbca7f (that sets edge_index for the
         * callback) is located at line 0.
         * So we need a way to get the previous instruction (llvm's SanCov does it in default callbacks):
         *  https://github.com/llvm/llvm-project/blob/main/llvm/tools/sancov/sancov.cpp#L769
         * LLVM's SanCov uses internal arch information to do that:
         *  https://github.com/llvm/llvm-project/blob/main/llvm/tools/sancov/sancov.cpp#L690
         */
        const uintptr_t addr = is_function_entry
            ?  pc_array[i]
            :  pc_array[i] - 1;

        edges_to_addrs[i / 2] = reinterpret_cast<void*>(addr);
    }

    // We don't symbolize the addresses right away, wait for CH application to load instead.
    // Starting now, we won't be able to log to Poco or std::cout;
}

void Writer::deinitRuntime()
{
    tasks_queue.wait();
    tasks_queue.finalize();
    writeCCRFooter();
    report_file.close();
}

void Writer::onServerInitialized()
{
    // Before server initialization we couldn't log data to Poco.
    base_log = &Poco::Logger::get(std::string{logger_base_name});

    symbolizeInstrumentedData();
}

void Writer::hit(EdgeIndex edge_index)
{
    /**
     * If we are copying data: the only place where edges_hit is used is pointer swap with a temporary vector.
     * In -O3 this code will surely be optimized to something like https://godbolt.org/z/8rerbshzT,
     * where only lines 32-36 swap the pointers, so we'll just probably count some old hits for new test.
     *
     */
    __atomic_add_fetch(&edges_hit[edge_index], 1, __ATOMIC_RELAXED);
}

namespace /// Symbolized data
{
struct AddrSym
{
    /**
     * Multiple callbacks can be called for a single line, e.g. ternary operator
     * a = foo ? bar() : baz() may produce two callbacks as it's basically an if-else block.
     * We track _lines_ coverage, not _addresses_ coverage, so we can hash local cache items by their unique lines.
     * However, test may register both of the callbacks, so we need to process all edge indices per line.
     */
    using Container = std::unordered_multimap<Line, EdgeIndex>;
};

struct FuncSym
{
    /**
     * Template instantiations get mangled into different names, e.g.
     * _ZNK2DB7DecimalIiE9convertToIlEET_v
     * _ZNK2DB7DecimalIlE9convertToIlEET_v
     * _ZNK2DB7DecimalInE9convertToIlEET_v
     *
     * We can't use predicate "all functions with same line are considered same" as it's easily
     * contradicted -- multiple functions can be invoked in the same source line, e.g. foo(bar(baz)):
     *
     * However, instantiations are located in different binary parts, so it's safe to use edge_index
     * as a hashing key (current solution is to just use a vector to avoid hash calculation).
     *
     * As a bonus, functions report view in HTML will show distinct counts for each instantiated template.
     */
    using Container = std::vector<FuncSym>;

    Line line;
    EdgeIndex index;
    std::string_view name;

    FuncSym(Line line_, EdgeIndex index_, std::string_view name_): line(line_), index(index_), name(name_) {}
};

template <bool is_func_cache>
static inline auto& getInstrumented(auto& info)
{
    if constexpr (is_func_cache)
        return info.instrumented_functions;
    else
        return info.instrumented_lines;
}
}

void Writer::symbolizeInstrumentedData()
{
    std::vector<EdgeIndex> function_indices(functions_count);
    std::vector<EdgeIndex> addr_indices(addrs_count);

    LocalCaches<FuncSym> func_caches(hardware_concurrency);
    LocalCaches<AddrSym> addr_caches(hardware_concurrency);

    for (size_t i = 0, j = 0, k = 0; i < edges_count; ++i)
        if (edge_is_func_entry[i])
            function_indices[j++] = i;
        else
            addr_indices[k++] = i;

    LOG_INFO(base_log,
        "Split addresses into function entries ({}) and normal ones ({}), {} total. "
        "Symbolizing addresses, thread pool of size {}",
        functions_count, addrs_count, edges_to_addrs.size(), hardware_concurrency);

    scheduleSymbolizationJobs<true, FuncSym>(func_caches, function_indices);

    tasks_queue.wait();

    LOG_INFO(base_log, "Symbolized all functions");

    /// Pre-populate addr_caches with already found source files.
    for (const auto& local_func_cache : func_caches)
        for (const auto& [source_rel_path, data] : local_func_cache)
            for (auto & local_addr_cache : addr_caches)
                local_addr_cache[source_rel_path] = {};

    scheduleSymbolizationJobs<false, AddrSym>(addr_caches, addr_indices);

    /// Merge functions data while other threads process addresses.
    mergeDataToCaches<true, FuncSym>(func_caches);

    tasks_queue.wait();

    mergeDataToCaches<false, AddrSym>(addr_caches);

    tasks_queue.changePoolSizeAndRespawnThreads(thread_pool_size_for_tests);

    LOG_INFO(base_log, "Symbolized all addresses");

    writeCCRHeaderToInternalBuffer();

    LOG_INFO(base_log, "Wrote report header to internal buffer");
}

void Writer::onChangedTestName(std::string old_test_name)
{
    /// Note: this function slows down setSetting, so it should be as fast as possible.

    if (test_index == 0) // Processing the first test
    {
        tests[0].name = std::move(old_test_name);
        ++test_index;
        return;
    }

    /// https://godbolt.org/z/qqTYdr5Gz, explicit 0 state generates slightly better assembly than without it.
    EdgesHit edges_hit_swap(edges_count, 0);

    const size_t test_that_finished = test_index - 1;
    const size_t test_that_will_run_next = test_index;
    ++test_index;

    edges_hit.swap(edges_hit_swap);

    tasks_queue.schedule([this, test_that_finished, edges = std::move(edges_hit_swap)] (auto) mutable
    {
        TestData & data = tests[test_that_finished];
        data.test_index = test_that_finished;
        data.log = &Poco::Logger::get(std::string{logger_base_name} + "." + data.name);
        prepareDataAndDump(data, edges);
    });

    if (!old_test_name.empty())
        tests[test_that_will_run_next].name = std::move(old_test_name);
    else
        deinitRuntime();
}

void Writer::prepareDataAndDump(TestData& test_data, const EdgesHit& hits)
{
    LOG_INFO(test_data.log, "Started filling internal structures");

    test_data.data.resize(source_files_cache.size());

    for (size_t edge_index = 0; edge_index < edges_count; ++edge_index)
    {
        const CallCount hit = hits[edge_index];

        if (!hit)
            continue;

        if (const EdgeInfo& info = edges_cache[edge_index]; info.isFunctionEntry())
            setOrIncrement(test_data.data[info.index].functions_hit, edge_index, hit);
        else
            setOrIncrement(test_data.data[info.index].lines_hit, info.line, hit);
    }

    LOG_INFO(test_data.log, "Finished filling internal structures");
    writeCCREntry(test_data);
}

void Writer::writeCCRHeaderToInternalBuffer()
{
    fmt::memory_buffer mb;
    mb.reserve(10 * 1024 * 1024); //small speedup

    /**
     * /absolute/path/to/ch/src/directory <- e.g.  /home/user/ch/src
     * FILES <source files count>
     * <source file 1 relative path from src/> <functions> <lines> <- e.g. Access/Myaccess.cpp 8 100
     * <sf 1 function 1 mangled name> <function start line> <function edge index>
     * <sf 1 function 2 mangled name> <function start line> <function edge index>
     * <sf 1 instrumented line 1>
     * <sf 1 instrumented line 2>
     * <source file 2 relative path from src/> <functions> <lines>
     * // Need of function edge index: multiple functions may be called in single line
     */
    fmt::format_to(mb, "{}\nFILES {}\n",
        clickhouse_src_dir_abs_path.string(),
        source_files_cache.size());

    for (const SourceFileInfo& file : source_files_cache)
    {
        fmt::format_to(mb, "{} {} {}\n",
            file.relative_path, file.instrumented_functions.size(), file.instrumented_lines.size());

        for (EdgeIndex index : file.instrumented_functions)
        {
            const EdgeInfo& func_info = edges_cache[index];
            fmt::format_to(mb, "{} {} {}\n", func_info.name, func_info.line, index);
        }

        if (!file.instrumented_lines.empty())
            fmt::format_to(mb, "{}\n", fmt::join(file.instrumented_lines, "\n"));
    }

    ccr_header_buffer = std::move(mb);
}

void Writer::writeCCREntry(const Writer::TestData& test_data)
{
    LOG_INFO(test_data.log, "Started writing test entry");

    /// Frequent writes to file are better than a single huge write, so don't use fmt::memory_buffer here.

    /**
     * TEST //Note -- test index is not written. Test name will be found in footer (TESTS section)
     * SOURCE <source file id> <functions count> <lines count>
     * <function 1 edge index> <call count>
     * <function 2 edge index> <call count>
     * <line 1 number> <call count>
     * <line 2 number> <call count>
     */
    fmt::print(report_file.file(), "TEST\n");

    for (size_t i = 0; i < test_data.data.size(); ++i)
    {
        const auto& source = test_data.data[i];

        if (source.functions_hit.empty() && source.lines_hit.empty())
            continue;

        fmt::print(report_file.file(), "SOURCE {} {} {}\n",
            i, source.functions_hit.size(), source.lines_hit.size());

        for (const auto [edge_index, call_count]: source.functions_hit)
            fmt::print(report_file.file(), "{} {}\n", edge_index, call_count);

        for (const auto [line, call_count]: source.lines_hit)
            fmt::print(report_file.file(), "{} {}\n", line, call_count);
    }

    LOG_INFO(test_data.log, "Finished writing test entry");
}

void Writer::writeCCRFooter()
{
    fmt::memory_buffer mb;

    /**
     * TESTS // Note -- no "tests_count", test names are till end of file.
     * <test 1 name>
     * <test 2 name>
     */

    fmt::format_to(mb, "TESTS\n");

    for (const auto& test : tests)
    {
        if (test.name.empty()) break;
        fmt::format_to(mb, "{}\n", test.name);
    }

    report_file.write(mb);
}

template <bool is_func_cache, class CacheItem>
void Writer::scheduleSymbolizationJobs(LocalCaches<CacheItem>& local_caches, const std::vector<EdgeIndex>& data)
{
    for (size_t k = 0; k < hardware_concurrency; ++k)
        tasks_queue.schedule([this, &local_caches, &data](size_t thread_index)
        {
            const size_t step = data.size() / hardware_concurrency;
            const size_t start_index = thread_index * step;
            const size_t end_index = std::min(start_index + step, data.size() - 1);
            const size_t count = end_index - start_index;

            const Poco::Logger * log = &Poco::Logger::get(
                std::string{logger_base_name} + "." + std::to_string(thread_index));

            LocalCache<CacheItem>& cache = local_caches[thread_index];
            cache.reserve(total_source_files_hint / hardware_concurrency);

            for (size_t i = start_index; i < end_index; ++i)
            {
                if (i % 2048 == 0)
                    LOG_INFO(log, "{}/{}", i - start_index, count);

                const EdgeIndex edge_index = data[i];

                const Dwarf::LocationInfo loc =
                    dwarf.findAddressForCoverageRuntime(uintptr_t(edges_to_addrs[edge_index]));

                SourceRelativePath source_rel_path = std::filesystem::path(loc.file.toString())
                    .lexically_relative(clickhouse_src_dir_abs_path)
                    .lexically_normal() // duplicated as single one removes only one foo/../sequence
                    .lexically_normal()
                    .string();

                const size_t line = loc.line;

                auto cache_it = cache.find(source_rel_path);

                if constexpr (is_func_cache)
                {
                    const std::string_view name = symbol_index->findSymbol(edges_to_addrs[edge_index])->name;

                    if (cache_it != cache.end())
                        cache_it->second.emplace_back(line, edge_index, name);
                    else
                        cache[std::move(source_rel_path)] = {{line, edge_index, name}};
                }
                else
                {
                    if (cache_it != cache.end())
                        cache_it->second.emplace(line, edge_index);
                    else
                        cache[std::move(source_rel_path)] = {{line, edge_index}};
                }
            }
        });
}

template<bool is_func_cache, class CacheItem>
void Writer::mergeDataToCaches(const LocalCaches<CacheItem>& data)
{
    LOG_INFO(base_log, "Started merging data to caches");

    for (const auto& cache : data)
        for (const auto& [source_rel_path, addrs_data] : cache)
        {
            SourceFileIndex source_index;

            if (auto it = source_rel_path_to_index.find(source_rel_path); it != source_rel_path_to_index.end())
                source_index = it->second;
            else
            {
                source_index = source_rel_path_to_index.size();
                source_rel_path_to_index.emplace(source_rel_path, source_index);
                source_files_cache.emplace_back(source_rel_path);
            }

            SourceFileInfo& info = source_files_cache[source_index];
            auto& instrumented = getInstrumented<is_func_cache>(info);

            instrumented.reserve(addrs_data.size());

            if constexpr (is_func_cache)
                for (auto [line, edge_index, name] : addrs_data)
                {
                    instrumented.push_back(edge_index);
                    edges_cache[edge_index] = {line, source_index, name};
                }
            else
            {
                std::unordered_set<Line> lines;

                for (auto [line, edge_index] : addrs_data)
                {
                    if (!lines.contains(line))
                        instrumented.push_back(line);

                    edges_cache[edge_index] = {line, source_index, {}};
                    lines.insert(line);
                }
            }
        }

    LOG_INFO(base_log, "Finished merging data to caches");
}

template void Writer::mergeDataToCaches<true, FuncSym>(const LocalCaches<FuncSym>&);
template void Writer::mergeDataToCaches<false, AddrSym>(const LocalCaches<AddrSym>&);
template void Writer::scheduleSymbolizationJobs<true, FuncSym>(LocalCaches<FuncSym>&, const std::vector<EdgeIndex>&);
template void Writer::scheduleSymbolizationJobs<false, AddrSym>(LocalCaches<AddrSym>&, const std::vector<EdgeIndex>&);
}
