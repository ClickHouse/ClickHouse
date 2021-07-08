#include "Coverage.h"
#include "CoverageDecls.h"

#include <unistd.h>
#include <signal.h>
#include <sys/mman.h>

#include <memory>
#include <utility>
#include <thread>

#include "common/logger_useful.h"

namespace coverage
{
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


void Writer::printErrnoAndThrow() const
{
    LOG_FATAL(base_log, "{}", strerror(errno));
    throw;
}

void Writer::onServerInitialized()
{
    // Writer constructor occurs before Poco internal structures initialization.
    base_log = &Poco::Logger::get(logger_base_name);

    // Some .sh tests spawn multiple server instances. However, only one server instance (the first) should write to
    // report file. If report file already exists, we silently start as server without coverage.
    if (access(report_path.c_str(), F_OK) == 0)
    {
        LOG_WARNING(base_log, "Report file {} already exists", report_path);
        return;
    }

    struct sigaction test_change = { .sa_handler = [](int) { Writer::instance().onTestFinished(); } };
    struct sigaction shut_down = { .sa_handler = [](int) { Writer::instance().onShutRuntime(); } };

    if (sigaction(SIGRTMIN + 1, &test_change, nullptr) == -1) printErrnoAndThrow();
    if (sigaction(SIGRTMIN + 2, &shut_down, nullptr) == -1) printErrnoAndThrow();

    report_file_fd = open(report_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (report_file_fd == -1) printErrnoAndThrow();

    LOG_INFO(base_log, "Opened report file {}", report_path);

    if (ftruncate(report_file_fd, report_file_size_upper_limit) == -1) printErrnoAndThrow();

    void * const ptr = mmap(nullptr, report_file_size_upper_limit, PROT_WRITE, MAP_SHARED, report_file_fd, 0);
    if (ptr == MAP_FAILED) printErrnoAndThrow();

    report_file_ptr = static_cast<uint32_t *>(ptr);

    mergeAndWriteHeader(symbolizeAddrsIntoLocalCaches());

    /// Testing script in docker (/docker/test/coverage/run.sh) waits for this message and starts tests afterwards.
    LOG_INFO(base_log, "Symbolized all addresses");
}

Writer::LocalCaches Writer::symbolizeAddrsIntoLocalCaches()
{
    const size_t hardware_concurrency { std::thread::hardware_concurrency() };

    LOG_INFO(base_log, "{} instrumented basic blocks. Using thread pool of size {}",
        instrumented_basic_blocks, hardware_concurrency);

    LocalCaches caches(hardware_concurrency);

    const SymbolIndexInstance symbol_index = SymbolIndex::instance();
    const Dwarf dwarf(symbol_index->getSelf()->elf);

    const size_t step = instrumented_basic_blocks / caches.size();

    std::vector<std::thread> workers(caches.size());

    for (size_t thread_index = 0; thread_index < caches.size(); ++thread_index)
        workers[thread_index] = std::thread{[this, step, thread_index, &dwarf, &cache = caches[thread_index]]
        {
            const size_t start_index = thread_index * step;
            const size_t end_index = std::min(start_index + step, instrumented_basic_blocks - 1);

            for (size_t i = start_index; i < end_index; ++i)
            {
                const Dwarf::LocationInfo loc = dwarf.findAddressForCoverageRuntime(instrumented_blocks_addrs[i]);

                const SourcePath src_path = loc.file.toString();
                const Line line = static_cast<Line>(loc.line);

                assert(!src_path.empty());

                if (i % 4096 == 0)
                    LOG_INFO(base_log, "{} {}/{}, file: {}:{}",
                        thread_index, i - start_index, end_index - start_index, src_path, line);

                const BBIndex bb_index = static_cast<BBIndex>(i);

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

    return caches;
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

    uint32_t * const ptr = report_file_ptr;

    ptr[0] = static_cast<uint32_t>(Magic::ReportHeader);
    ptr[++report_file_pos] = source_files.size();

    for (const auto& [path, instrumented_blocks] : source_files)
    {
        const uint32_t path_size = path.size();
        const uint32_t path_mod4 = path_size % 4;
        const uint32_t path_padding = (path_mod4 == 0) ? 0 : (4 - path_mod4);
        const uint32_t path_word_len = (path_size + path_padding) / 4;

        ptr[++report_file_pos] = path_word_len;

        char * const report_ptr_char = reinterpret_cast<char *>(&ptr[++report_file_pos]);

        memcpy(report_ptr_char, path.c_str(), path_size);
        memset(report_ptr_char + path_size, 0, path_padding);

        report_file_pos += path_word_len;

        ptr[report_file_pos] = instrumented_blocks.size(); // no increment here as already on empty position

        for (BBIndex index : instrumented_blocks)
        {
            ptr[++report_file_pos] = index;
            ptr[++report_file_pos] = instrumented_blocks_start_lines[index];
        }
    }
}

void Writer::onTestFinished()
{
    report_file_ptr[++report_file_pos] = static_cast<uint32_t>(Magic::TestEntry);

    for (size_t bb_index = 0; bb_index < instrumented_basic_blocks; ++bb_index)
        if (current[bb_index])
        {
            current[bb_index] = false;
            report_file_ptr[++report_file_pos] = bb_index;
        }
}

void Writer::onShutRuntime()
{
    msync(report_file_ptr, report_file_size_upper_limit, MS_SYNC);
    munmap(report_file_ptr, report_file_size_upper_limit);

    const size_t file_real_size = ++report_file_pos * sizeof(uint32_t);

    if (ftruncate(report_file_fd, file_real_size) == -1) printErrnoAndThrow();
    if (close(report_file_fd) == -1) printErrnoAndThrow();
}
}
