#include "Coverage.h"

#include <memory>
#include <utility>

#include <fmt/core.h>
#include <fmt/format.h>

#include "Common/ProfileEvents.h"
#include "Common/ErrorCodes.h"
#include "Common/Exception.h"

namespace DB::ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int FILE_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
}

namespace detail
{
using Exception = DB::Exception;

namespace
{
// Unused in Darwin and FreeBSD builds.
[[maybe_unused]] inline SymbolIndexInstance getInstanceAndInitGlobalCounters()
{
    /**
     * Writer is a singleton, so it initializes statically.
     * SymbolIndex uses a MMapReadBufferFromFile which uses ProfileEvents.
     * If no thread was found in the events profiler, a global variable global_counters is used.
     *
     * This variable may get initialized after Writer (static initialization order fiasco).
     * In fact, __sanitizer_cov_trace_pc_guard_init is called before global_counters init.
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

void TaskQueue::start()
{
    worker = std::thread([this]
    {
        while (true)
        {
            Task task;

            {
                std::unique_lock lock(mutex);

                task_or_shutdown.wait(lock, [this] { return shutdown || !tasks.empty(); });

                if (tasks.empty())
                    return;

                task = std::move(tasks.front());
                tasks.pop();
            }

            task();
        }
    });
}

void TaskQueue::wait()
{
    size_t active_tasks = 0;

    {
        std::lock_guard lock(mutex);
        active_tasks = tasks.size();
        shutdown = true;
    }

    // If queue has n tasks left, we need to notify it n times
    // (to process all tasks) and one extra time to shut down.
    for (size_t i = 0; i < active_tasks + 1; ++i)
        task_or_shutdown.notify_one();

    if (worker.joinable())
        worker.join();
}

Writer::Writer()
    :
#if NON_ELF_BUILD
      symbol_index(),
      dwarf()
#else
      symbol_index(getInstanceAndInitGlobalCounters()),
      dwarf(symbol_index->getSelf()->elf)
#endif
    {}

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
    if (is_client)
        return;

    tasks_queue.wait();
    writeCCRFooter();
    report_file.close();

    LOG_INFO(base_log, "Shut down runtime");
}

void Writer::setTestsCount(size_t tests_count)
{
    // This is an upper bound (as we don't know which tests will be skipped before running them).
    tests.resize(tests_count);
}

void Writer::hit(EdgeIndex index)
{
    /**
     * If we are copying data: the only place where edges_hit is used is pointer swap with a temporary vector.
     * In -O3 this code will surely be optimized to something like https://godbolt.org/z/8rerbshzT,
     * where only lines 32-36 swap the pointers, so we'll just probably count some old hits for new test.
     *
     * If we are not copying data: no race as it's just assignment.
     */
    edges_hit[index] = 1;
}

void Writer::onClientInitialized()
{
    is_client = true;
}

void Writer::onServerInitialized()
{
    // We can't log data using Poco before server initialization as Writer constructor gets called in
    // sanitizer callback which occurs before Poco internal structures initialization.
    base_log = &Poco::Logger::get(logger_base_name);

    // Some functional .sh tests spawn own server instances.
    // In coverage mode it leads to concurrent file writes (file write + open in "w" truncate mode, to be precise).
    // This leads to data corruption.
    // To prevent such situation, target file is not allowed to exist by server start.
    if (std::filesystem::exists(report_path))
        throw Exception(DB::ErrorCodes::FILE_ALREADY_EXISTS, "Report file {} already exists", report_path);

    // fwrite also cannot be called before server initialization (some internal state is left uninitialized if we
    // try to write file in PC table callback).
    report_file.set(report_path, "w");

    if (report_file.file() == nullptr)
        throw Exception(DB::ErrorCodes::CANNOT_OPEN_FILE, "Failed to open {}: {}", report_path, strerror(errno));
    else
        LOG_INFO(base_log, "Opened report file {}", report_path);

    tasks_queue.start();

    symbolizeInstrumentedData();
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
inline auto& getInstrumented(auto& info)
{
    if constexpr (is_func_cache)
        return info.instrumented_functions;
    else
        return info.instrumented_lines;
}

inline void waitFor(Threads& threads)
{
    for (auto & thread : threads)
        if (thread.joinable())
            thread.join();
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
        "Using thread pool of size {}. ",
        functions_count, addrs_count, edges_to_addrs.size(), hardware_concurrency);

    /// Schedule function symbolization jobs and wait for them to finish
    Threads func_thread_pool = scheduleSymbolizationJobs<true, FuncSym>(func_caches, function_indices);
    waitFor(func_thread_pool);

    LOG_INFO(base_log, "Symbolized all functions");

    /// Pre-populate addr_caches with already found source files.
    for (const auto& local_func_cache : func_caches)
        for (const auto& [source_rel_path, data] : local_func_cache)
            for (auto & local_addr_cache : addr_caches)
                local_addr_cache[source_rel_path] = {};

    Threads addr_thread_pool = scheduleSymbolizationJobs<false, AddrSym>(addr_caches, addr_indices);

    /// Merge functions data in main thread while other threads process addresses.
    mergeDataToCaches<true, FuncSym>(func_caches);
    waitFor(addr_thread_pool);

    mergeDataToCaches<false, AddrSym>(addr_caches);

    if (const size_t sf_count = source_files_cache.size(); sf_count < 100)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR,
            "Not enough source files ({} < 100), must be a symbolizer bug", sf_count);
    else
        LOG_INFO(base_log, "Found {} source files", sf_count);

    writeCCRHeader();

    // Testing script in docker (/docker/test/coverage/run.sh) waits for this message and starts tests afterwards.
    // If we place it before writeCCRHeader(), concurrent writes to report file will happen and data will corrupt.
    LOG_INFO(base_log, "Symbolized all addresses");
}

void Writer::onChangedTestName(std::string old_test_name)
{
    /// Note: this function slows down setSetting, so it should be as fast as possible.

    if (is_client)
        return;

    if (test_index == 0) // Processing the first test
    {
        tests[0].name = std::move(old_test_name);
        ++test_index;
        return;
    }

    const size_t test_that_will_run_next = test_index;
    const size_t test_that_finished = test_that_will_run_next - 1;
    ++test_index;

    EdgesHit edges_hit_swap(edges_count, 0);

    edges_hit.swap(edges_hit_swap);

    tasks_queue.schedule([this, test_that_finished, edges = std::move(edges_hit_swap)]
    {
        TestData & data = tests[test_that_finished];
        data.test_index = test_that_finished;
        data.log = &Poco::Logger::get(logger_base_name + "." + data.name);
        prepareDataAndDump(data, edges);
    });

    if (!old_test_name.empty())
        tests[test_that_will_run_next].name = std::move(old_test_name);
    else
        deinitRuntime();
}

void Writer::prepareDataAndDump(TestData& test_data, const EdgesHit& hits)
{
    LOG_INFO(test_data.log, "Started processing test entry");

    test_data.data.resize(source_files_cache.size());

    for (size_t edge_index = 0; edge_index < edges_count; ++edge_index)
        if (hits[edge_index]) //char to bool conversion
        {
            if (const EdgeInfo& info = edges_cache[edge_index]; info.isFunctionEntry())
                test_data.data[info.index].functions_hit.push_back(edge_index);
            else
                test_data.data[info.index].lines_hit.push_back(info.line);
        }

    writeCCREntry(test_data);
}

void Writer::writeCCRHeader()
{
    /**
     * FILES <source files count>
     * <source file 1 absolute path> <functions> <lines>
     * <sf 1 function 1 mangled name> <function start line> <function edge index>
     * <sf 1 function 2 mangled name> <function start line> <function edge index>
     * <sf 1 instrumented line 1>
     * <sf 1 instrumented line 2>
     * <source file 2 path> <functions> <lines>
     */
    fmt::print(report_file.file(), "FILES {}\n", source_files_cache.size());

    for (const SourceFileInfo& file : source_files_cache)
    {
        fmt::print(report_file.file(), "{} {} {}\n",
            file.relative_path, file.instrumented_functions.size(), file.instrumented_lines.size());

        for (EdgeIndex index : file.instrumented_functions)
        {
            // Note: need of function edge index: multiple functions may be called in single line
            const EdgeInfo& func_info = edges_cache[index];
            fmt::print(report_file.file(), "{} {} {}\n", func_info.name, func_info.line, index);
        }

        if (!file.instrumented_lines.empty())
            fmt::print(report_file.file(), "{}\n", fmt::join(file.instrumented_lines, "\n"));
    }

    fflush(report_file.file());

    LOG_INFO(base_log, "Wrote CCR header");
}

void Writer::writeCCREntry(const Writer::TestData& test_data)
{
    /// Frequent writes to file are better than a single huge write, so don't use fmt::memory_buffer here.

    /**
     * TEST
     * SOURCE <source file id>
     * <function 1 edge index> <function 2 edge index>
     * <line 1 number> <line 2 number>
     */
    fmt::print(report_file.file(), "TEST\n");

    for (size_t i = 0; i < test_data.data.size(); ++i)
    {
        const TestData::SourceData& source = test_data.data[i];
        const auto& funcs = source.functions_hit;
        const auto& lines = source.lines_hit;

        if (funcs.empty() && lines.empty())
            continue;

        fmt::print(report_file.file(), "SOURCE {}\n{}\n{}\n", i, fmt::join(funcs, " "), fmt::join(lines, " "));
    }

    LOG_INFO(test_data.log, "Finished processing test entry");
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
Threads Writer::scheduleSymbolizationJobs(LocalCaches<CacheItem>& local_caches, const std::vector<EdgeIndex>& data)
{
    Threads out;
    out.reserve(hardware_concurrency);

    // Each worker symbolizes an address range. Ranges are uniformly distributed.

    for (size_t thread_index = 0; thread_index < hardware_concurrency; ++thread_index)
        out.emplace_back([this, &local_caches, &data, thread_index]()
        {
            const size_t step = data.size() / hardware_concurrency;
            const size_t start_index = thread_index * step;
            const size_t end_index = std::min(start_index + step, data.size() - 1);
            const size_t count = end_index - start_index;

            const Poco::Logger * log = &Poco::Logger::get(
                String{logger_base_name} + "." + std::to_string(thread_index));

            LocalCache<CacheItem>& cache = local_caches[thread_index];
            cache.reserve(total_source_files_hint / hardware_concurrency);

            for (size_t i = start_index; i < end_index; ++i)
            {
                const EdgeIndex edge_index = data[i];

                const auto loc = dwarf.findAddressForCoverageRuntime(uintptr_t(edges_to_addrs[edge_index]));
                SourceRelativePath src_path = std::filesystem::path(loc.file.toString()).string();

                if (src_path.empty())
                    throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Internal symbolizer error");

                if (i % 2048 == 0)
                    LOG_INFO(log, "{}/{}, file: {}",
                        i - start_index, count, src_path);

                const int line = static_cast<int>(loc.line);

                auto cache_it = cache.find(src_path);

                if constexpr (is_func_cache)
                {
                    const std::string_view name = symbol_index->findSymbol(edges_to_addrs[edge_index])->name;

                    if (cache_it != cache.end())
                        cache_it->second.emplace_back(line, edge_index, name);
                    else
                        cache[std::move(src_path)] = {{line, edge_index, name}};
                }
                else
                {
                    if (cache_it != cache.end())
                        cache_it->second.emplace(line, edge_index);
                    else
                        cache[std::move(src_path)] = {{line, edge_index}};
                }
            }
        });

    LOG_INFO(base_log, "Scheduled symbolization jobs");
    return out;
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

                    // lines is a vector, so we need to check for duplicates beforehand
                    lines.insert(line);
                }
            }
        }

    LOG_INFO(base_log, "Finished merging data to caches");
}

template void Writer::mergeDataToCaches<true, FuncSym>(const LocalCaches<FuncSym>&);
template void Writer::mergeDataToCaches<false, AddrSym>(const LocalCaches<AddrSym>&);
template Threads Writer::scheduleSymbolizationJobs<true, FuncSym>(LocalCaches<FuncSym>&, const std::vector<EdgeIndex>&);
template Threads Writer::scheduleSymbolizationJobs<false, AddrSym>(LocalCaches<AddrSym>&, const std::vector<EdgeIndex>&);
}
