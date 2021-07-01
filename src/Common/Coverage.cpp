#include "Coverage.h"

#include <unistd.h>

#include <memory>
#include <utility>
#include <thread>

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
static const size_t hardware_concurrency { std::thread::hardware_concurrency() };
static const String logger_base_name {"Coverage"};

using DB::Exception;

#if NON_ELF_BUILD
    Writer::Writer() : symbol_index(), dwarf() {}
#else

inline SymbolIndexInstance getInstanceAndInitGlobalCounters()
{
    /**
     * Writer is a singleton, so it initializes statically.
     * SymbolIndex uses a MMapReadBufferFromFile which uses ProfileEvents.
     * If no thread was found in the events profiler, a global variable global_counters is used.
     *
     * This variable may get initialized after Writer (static initialization order fiasco).
     * In fact, sanitizer callback is evaluated before global_counters init.
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

Writer::Writer() :
      symbol_index(getInstanceAndInitGlobalCounters()),
      dwarf(symbol_index->getSelf()->elf) {}

#endif

Writer& Writer::instance()
{
    static Writer w;
    return w;
}

void Writer::pcTableCallback(const Addr * start, const Addr * end) noexcept
{
    const size_t bb_pairs = end - start;
    instrumented_basic_blocks = bb_pairs / 2;

    instrumented_blocks_addrs.resize(instrumented_basic_blocks);
    instrumented_blocks_start_lines.resize(instrumented_basic_blocks);

    for (size_t i = 0; i < instrumented_basic_blocks; ++i)
        /// Real address for non-function entries is the previous instruction, see implementation in clang:
        /// https://github.com/llvm/llvm-project/blob/main/llvm/tools/sancov/sancov.cpp#L768
        instrumented_blocks_addrs[i] = start[2 * i] - !(start[2 * i + 1] & 1);
}

void Writer::countersCallback(bool * start, bool * end) noexcept
{
    std::fill(start, end, false);
    current = {start, end};
}

void Writer::onClientInitialized() noexcept
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

    // fwrite also can't be called before server initialization (some internal state is left uninitialized if we
    // try to write file in PC table callback).
    if (report_file.set(report_path, "w") == nullptr)
        throw Exception(DB::ErrorCodes::CANNOT_OPEN_FILE,
            "Failed to open {} in write mode: {}", report_path, strerror(errno));

    LOG_INFO(base_log, "Opened report file {}", report_path);

    symbolizeInstrumentedData();
}

void Writer::symbolizeInstrumentedData()
{
    LocalCaches caches(hardware_concurrency);

    LOG_INFO(base_log, "{} instrumented basic blocks. Using thread pool of size {}",
        instrumented_basic_blocks, hardware_concurrency);

    symbolizeAddrsIntoLocalCaches(caches);
    mergeIntoGlobalCache(caches);

    LOG_INFO(base_log, "Found {} source files", source_files.size());
    assert(source_files.size() > 2000);

    writeReportHeader();

    /// Testing script in docker (/docker/test/coverage/run.sh) waits for this message and starts tests afterwards.
    LOG_INFO(base_log, "Symbolized all addresses");
}

void Writer::symbolizeAddrsIntoLocalCaches(LocalCaches& caches)
{
    std::vector<std::thread> workers(hardware_concurrency);

    for (size_t thread_index = 0; thread_index < hardware_concurrency; ++thread_index)
        workers[thread_index] = std::thread{[this, thread_index, &cache = caches[thread_index]]
        {
            const size_t step = instrumented_basic_blocks / hardware_concurrency;
            const size_t start_index = thread_index * step;
            const size_t end_index = std::min(start_index + step, instrumented_basic_blocks - 1);

            const Poco::Logger * log = &Poco::Logger::get(fmt::format("{}.{}", logger_base_name, thread_index));

            for (size_t i = start_index; i < end_index; ++i)
            {
                const BBIndex bb_index = static_cast<BBIndex>(i);
                const Dwarf::LocationInfo loc = dwarf.findAddressForCoverageRuntime(instrumented_blocks_addrs[i]);

                const SourcePath src_path = loc.file.toString();
                const Line line = static_cast<Line>(loc.line);

                assert(!src_path.empty());

                if (i % 4096 == 0)
                    LOG_INFO(log, "{}/{}, file: {}:{}", i - start_index, end_index - start_index, src_path, line);

                if (auto cache_it = cache.find(src_path); cache_it != cache.end())
                    cache_it->second.emplace_back(bb_index, line);
                else
                    cache[src_path] = {{bb_index, line}};
            }
        }};

    for (auto & worker : workers)
        worker.join();
}

void Writer::mergeIntoGlobalCache(const LocalCaches& caches)
{
    std::unordered_map<SourcePath, SourceIndex> path_to_index;

    for (const auto & cache : caches)
        for (const auto & [source_path, symbolized_data] : cache)
        {
            SourceIndex source_index;

            if (auto it = path_to_index.find(source_path); it != path_to_index.end())
                source_index = it->second;
            else
            {
                source_index = path_to_index.size();
                path_to_index.emplace(source_path, source_index);
                source_files.push_back(std::make_pair(source_path, Blocks{}));
            }

            Blocks & instrumented = source_files[source_index].second;

            for (const auto & [bb_index, start_line] : symbolized_data)
            {
                instrumented_blocks_start_lines[bb_index] = start_line;
                instrumented.push_back(bb_index);
            }
        }
}

void Writer::onChangedTestName(String name)
{
    if (unlikely(is_client))
        return;

    name.swap(test_name);

    const String& finished_test = name;
    const String& next_test = test_name;

    if (unlikely(finished_test.empty())) // Processing first test
        return;

    report_file.write(Magic::TestEntry);
    report_file.write(finished_test);

    for (size_t i = 0; i < instrumented_basic_blocks; ++i) // TODO SIMD
        if (current[i])
        {
            current[i] = false;
            report_file.write(i);
        }

    if (unlikely(next_test.empty())) // Finished testing
    {
        report_file.close();
        LOG_INFO(base_log, "Shut down runtime");
    }
}

void Writer::writeReportHeader() noexcept
{
    report_file.write(Magic::ReportHeader);
    report_file.write(source_files.size());

    for (const auto& [path, instrumented_blocks] : source_files)
    {
        report_file.write(path);
        report_file.write(instrumented_blocks.size());

        for (BBIndex index : instrumented_blocks)
        {
            report_file.write(index);
            report_file.write(instrumented_blocks_start_lines[index]);
        }
    }
}
}
