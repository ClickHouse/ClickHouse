#pragma once

#include <span>
#include <unordered_map>
#include <Poco/Logger.h>

namespace coverage
{
using Addr = uintptr_t;
using Line = int;
using BBIndex = int;
using SourcePath = std::string;

class Writer
{
public:
    static Writer& instance();

    constexpr void pcTableCallback(const Addr * start, const Addr * end) { instrumented_blocks_pairs = {start, end}; }
    constexpr void countersCallback(bool * start, bool * end) { current = {start, end}; }

    void onServerInitialized();

    void onTestFinished();
    void onShutRuntime();

private:
    constexpr Writer() = default;

    const Poco::Logger * base_log {nullptr};

    std::span<const Addr> instrumented_blocks_pairs;
    size_t instrumented_basic_blocks {0};

    std::span<bool> current; /// Compiler-provided array of flags for each instrumented BB.

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
