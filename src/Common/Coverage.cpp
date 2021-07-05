#include "Coverage.h"
#include "CoverageDecls.h"

#include <unistd.h>
#include <signal.h>
#include <sys/mman.h>

#include <memory>
#include <utility>
#include <thread>

#include <fmt/core.h>
#include <fmt/format.h>

#include "common/logger_useful.h"

namespace coverage
{
static const size_t hardware_concurrency { std::thread::hardware_concurrency() };
static const String logger_base_name {"Coverage"};

enum class Magic : uint32_t
{
    ReportHeader = 0xcafefefe,
    TestEntry = 0xcafecafe
};

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

    for (size_t i = 0; i < bb_pairs / 2; ++i)
        /// Real address for non-function entries is the previous instruction, see implementation in clang:
        /// https://github.com/llvm/llvm-project/blob/main/llvm/tools/sancov/sancov.cpp#L768
        instrumented_blocks_addrs[i] = start[2 * i] - !(start[2 * i + 1] & 1);
}

void Writer::countersCallback(bool * start, bool * end) noexcept
{
    std::fill(start, end, false);
    current = {start, end};
}

void Writer::onServerInitialized()
{
    // Some .sh tests spawn multiple server instances. However, only one server instance (the first) should write to
    // report file. If report file already exists, we silently start as server without coverage.
    if (access(report_path.c_str(), F_OK) != 0)
    {
        LOG_WARNING(base_log, "Report file already exists");
        return;
    }

    // Writer constructor occurs before Poco internal structures initialization.
    base_log = &Poco::Logger::get(logger_base_name);

    struct sigaction test_change = { .sa_handler = [](int) { Writer::instance().onTestFinished(); } };
    struct sigaction shut_down = { .sa_handler = [](int) { Writer::instance().onShutRuntime(); } };

    [[maybe_unused]] const int test_change_ret = sigaction(SIGRTMIN + 1, &test_change, nullptr);
    [[maybe_unused]] const int shut_down_ret = sigaction(SIGRTMIN + 2, &shut_down, nullptr);

    assert(test_change_ret == 0);
    assert(shut_down_ret == 0);

    const int fd = creat(report_path.c_str(), 0666);
    assert(fd != -1);

    report_file_ptr = mmap(nullptr, report_file_size, PROT_WRITE, MAP_SHARED, fd, 0 /*offset*/);
    assert(report_file_ptr != static_cast<void *>(-1));

    [[maybe_unused]] const int ret = close(fd);
    assert(ret == 0);

    symbolizeInstrumentedData();
}

void Writer::symbolizeInstrumentedData()
{
    LocalCaches caches(hardware_concurrency);

    LOG_INFO(base_log, "{} instrumented basic blocks. Using thread pool of size {}",
        instrumented_basic_blocks, hardware_concurrency);

    symbolizeAddrsIntoLocalCaches(caches);
    mergeAndWriteHeader(caches);

    /// Testing script in docker (/docker/test/coverage/run.sh) waits for this message and starts tests afterwards.
    LOG_INFO(base_log, "Symbolized all addresses");
}

void Writer::symbolizeAddrsIntoLocalCaches(LocalCaches& caches)
{
    const SymbolIndexInstance symbol_index;
    const Dwarf dwarf(symbol_index->getSelf()->elf);

    std::vector<std::thread> workers(hardware_concurrency);

    for (size_t thread_index = 0; thread_index < hardware_concurrency; ++thread_index)
        workers[thread_index] = std::thread{[this, thread_index, &dwarf, &cache = caches[thread_index]]
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

    instrumented_blocks_addrs.clear();
    instrumented_blocks_addrs.shrink_to_fit();
}

void Writer::mergeAndWriteHeader(const LocalCaches& caches)
{
    using SourceIndex = int;
    using InstrumentedBlocks = std::vector<BBIndex>;
    using SourceInfo = std::pair<SourcePath, InstrumentedBlocks>;

    std::unordered_map<SourcePath, SourceIndex> path_to_index;
    std::vector<SourceInfo> source_files;
    std::vector<Line> instrumented_blocks_start_lines(instrumented_basic_blocks);

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
                source_files.push_back(std::make_pair(source_path, InstrumentedBlocks{}));
            }

            InstrumentedBlocks & instrumented = source_files[source_index].second;

            for (const auto & [bb_index, start_line] : symbolized_data)
            {
                instrumented_blocks_start_lines[bb_index] = start_line;
                instrumented.push_back(bb_index);
            }
        }

    LOG_INFO(base_log, "Found {} source files", source_files.size());
    assert(source_files.size() > 2000);

    uint32_t * const report_ptr = reinterpret_cast<uint32_t*>(report_file_ptr);

    report_ptr[0] = static_cast<uint32_t>(Magic::ReportHeader);
    report_ptr[++report_file_pos] = source_files.size();

    for (const auto& [path, instrumented_blocks] : source_files)
    {
        const uint32_t path_size = path.size();
        const uint32_t path_padding = (4 - path_size % 4) % 4;
        const uint32_t path_word_len = (path_size + path_padding) / 4;

        report_ptr[++report_file_pos] = path_word_len;

        char * report_ptr_char = reinterpret_cast<char * >(&report_ptr[++report_file_pos]);

        for (const char c : path) *report_ptr_char++ = c;
        for (size_t i = 0; i < path_padding; ++i) *report_ptr_char++ = '\0';

        report_file_pos += path_word_len;

        report_ptr[++report_file_pos] = instrumented_blocks.size();

        for (BBIndex index : instrumented_blocks)
        {
            report_ptr[++report_file_pos] = index;
            report_ptr[++report_file_pos] = instrumented_blocks_start_lines[index];
        }
    }
}

void Writer::onTestFinished()
{
    uint32_t * const report_ptr = reinterpret_cast<uint32_t*>(report_file_ptr);

    report_ptr[++report_file_pos] = static_cast<uint32_t>(Magic::TestEntry);

    for (size_t bb_index = 0; bb_index < instrumented_basic_blocks; ++bb_index)
        if (current[bb_index])
        {
            current[bb_index] = false;
            report_ptr[++report_file_pos] = bb_index;
        }
}

void Writer::onShutRuntime()
{
    msync(report_file_ptr, report_file_size, MS_SYNC);
    munmap(report_file_ptr, report_file_size);
}
}
