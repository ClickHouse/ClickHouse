#pragma once

#include <span>
#include <unordered_map>
#include <Poco/Logger.h>
#include <common/types.h>

namespace coverage
{
static const String report_path { "/report.ccr" }; // Change if you want to test runtime outside of Docker

using Addr = uintptr_t;
using Line = int;
using BBIndex = int;
using SourcePath = String;

class Writer
{
public:
    static Writer& instance();

    void pcTableCallback(const Addr * start, const Addr * end) noexcept;
    void countersCallback(bool * start, bool * end) noexcept;

    void onServerInitialized();

    void onTestFinished();
    void onShutRuntime();

private:
    Writer() = default;

    const Poco::Logger * base_log {nullptr};

    std::vector<Addr> instrumented_blocks_addrs;
    size_t instrumented_basic_blocks {0};

    std::span<bool> current; /// Compiler-provided array of flags for each instrumented BB.

    static constexpr size_t report_file_size_upper_limit = 150 * 1024 * 1024;
    int report_file_fd {-1};
    uint32_t * report_file_ptr {nullptr};
    size_t report_file_pos {0};

    using SourceSymbolizedData = std::vector<std::pair<BBIndex, Line>>;
    using LocalCache = std::unordered_map<SourcePath, SourceSymbolizedData>;
    using LocalCaches = std::vector<LocalCache>;

    LocalCaches symbolizeAddrsIntoLocalCaches();
    void mergeAndWriteHeader(const LocalCaches& caches);

    [[noreturn]] void printErrnoAndThrow() const;
};
}
