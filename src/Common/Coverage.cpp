#include "Coverage.h"

#include <unistd.h>

#include <memory>
#include <utility>

#include <fmt/core.h>
#include <fmt/format.h>

#include "common/logger_useful.h"

#include "Common/ProfileEvents.h"
#include "Common/ErrorCodes.h"
#include "Common/Exception.h"

namespace DB::ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int FILE_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
}

namespace coverage
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
    static Writer w;
    return w;
}

void Writer::initializeRuntime(const uintptr_t * pc_array, const uintptr_t * pc_array_end)
{
    const size_t bb_pairs = pc_array_end - pc_array;
    bb_count = bb_pairs / 2;

    bb_cache.resize(bb_count);

    for (size_t i = 0; i < bb_count; i += 2)
        /// Real address is the previous instruction, see implementation in clang:
        /// https://github.com/llvm/llvm-project/blob/main/llvm/tools/sancov/sancov.cpp#L768
        bb_cache[i].addr = reinterpret_cast<Addr>(pc_array[i] - 1);
}

void Writer::hitArray(bool * start, bool * end)
{
    std::fill(start, end, false);
    test_data = {start, end};
    bb_count = test_data.size();
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
    // This is an upper bound as we don't know which tests will be skipped before running them.
    tests.resize(tests_count);
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
    // In coverage mode it leads to concurrent file writes (file write + open in "w" truncate mode, to be precise),
    // which results in data corruption.
    // To prevent such situation, target file is not allowed to exist at server start.
    if (access(report_path.c_str(), F_OK) == 0)
        throw Exception(DB::ErrorCodes::FILE_ALREADY_EXISTS, "Report file {} already exists", report_path);

    // fwrite also cannot be called before server initialization (some internal state is left uninitialized if we
    // try to write file in PC table callback).
    if (report_file.set(report_path, "w") == nullptr)
        throw Exception(DB::ErrorCodes::CANNOT_OPEN_FILE,
            "Failed to open {} in write mode: {}", report_path, strerror(errno));
    else
        LOG_INFO(base_log, "Opened report file {}", report_path);

    tasks_queue.start();

    symbolizeInstrumentedData();
}

void Writer::symbolizeInstrumentedData()
{
    LocalCaches caches(hardware_concurrency);

    LOG_INFO(base_log, "{} instrumented basic blocks. Using thread pool of size {}", bb_count, hardware_concurrency);

    symbolizeAddrsIntoLocalCaches(caches);
    mergeIntoGlobalCache(caches);

    if (const size_t sf_count = source_files_cache.size(); sf_count < 1000)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR,
            "Not enough source files ({} < 1000), must be a symbolizer bug", sf_count);
    else
        LOG_INFO(base_log, "Found {} source files", sf_count);

    writeCCRHeader();

    // Testing script in docker (/docker/test/coverage/run.sh) waits for this message and starts tests afterwards.
    // If we place it before function return, concurrent writes to report file will happen and data will corrupt.
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

    for (size_t bb_index = 0; bb_index < bb_count; ++bb_index)
        if (hits[bb_index]) //char to bool conversion
            test_data.data[bb_cache[bb_index].source_index].push_back(bb_index);

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

void symbolizeAddrsIntoLocalCaches(LocalCaches& caches);
{
    // Each worker symbolizes an address range. Ranges are distributed uniformly.

    std::vector<std::thread> workers;
    workers.reserve(hardware_concurrency);

    const size_t step = bb_count / hardware_concurrency;

    for (size_t thread_index = 0; thread_index < hardware_concurrency; ++thread_index)
        workers.emplace_back([this, step, thread_index, &cache = caches[thread_index]]
        {
            const size_t start_index = thread_index * step;
            const size_t end_index = std::min(start_index + step, bb_count - 1);

            const Poco::Logger * log = &Poco::Logger::get(fmt::format("{}.{}", logger_base_name, thread_index));

            for (size_t i = start_index; i < end_index; ++i)
            {
                const Dwarf::LocationInfo loc = dwarf.findAddressForCoverageRuntime(uintptr_t(bb_cache[i].addr));

                SourcePath src_path = loc.file.toString();

                if (src_path.empty())
                    throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Internal symbolizer error");

                if (i % 4096 == 0)
                    LOG_INFO(log, "{}/{}, file: {}", i - start_index, end_index - start_index, src_path);

                const int line = static_cast<int>(loc.line);

                if (auto cache_it = cache.find(src_path); cache_it != cache.end())
                    cache_it->second.emplace(line, edge_index);
                else
                    cache[std::move(src_path)] = {{line, edge_index}};
            }
        });

    for (auto & worker : workers)
        if (worker.joinable())
            worker.join();
}

void Writer::mergeIntoGlobalCache(const LocalCaches& caches)
{
    std::unordered_map<SourcePath, SourceIndex> path_to_index;

    for (const auto& cache : caches)
        for (const auto& [source_path, symbolized_data] : cache)
        {
            SourceIndex source_index;

            if (auto it = path_to_index.find(source_path); it != path_to_index.end())
                source_index = it->second;
            else
            {
                source_index = path_to_index.size();
                path_to_index.emplace(source_path, source_index);
                source_files_cache.emplace_back(source_path);
            }

            auto& instrumented_lines = source_files_cache[source_index].instrumented;

            for (const auto [bb_index, start_line] : symbolized_data)
                bb_cache[bb.index] = { bb_cache[bb.index].addr, start_line, source_index };
        }
}
}
