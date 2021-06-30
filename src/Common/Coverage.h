#pragma once

#include <span>
#include <unordered_map>

#include <Poco/Logger.h>

#include <common/types.h>

#include "CoverageDecls.h"

namespace coverage
{
/// If you want to test runtime outside of Docker, change this variable.
static const String report_path { "/report.ccr" };

static const size_t hardware_concurrency { std::thread::hardware_concurrency() };
static constexpr std::string_view setting_test_name = "coverage_test_name";
static constexpr std::string_view setting_tests_count = "coverage_tests_count";
static const String logger_base_name {"Coverage"};

using Addr = void *;

using Line = int;

using SourceIndex = int;
using BBIndex = int;
using AddrIndex = int;

using SourcePath = String; // source file path
using Blocks = std::vector<BBIndex>;

struct SourceInfo
{
    SourcePath path;
    Blocks instrumented_blocks = {};
};

struct BBInfo
{
    Addr addr;
    Line start_line = 0;
    SourceIndex source_index = 0;
};

struct TestData
{
    const Poco::Logger * log;

    size_t test_index;
    std::string name; // If empty, there is no data for test.
    std::vector<Blocks> source_files_data; // [i] ~ hit blocks for source file with index i.
};

class Writer
{
public:
    static Writer& instance();

    void initializeRuntime(const uintptr_t * pc_array, const uintptr_t * pc_array_end);

    void hitArray(bool * start, bool * end);

    void onServerInitialized();

    void onClientInitialized();

    void setTestsCount(size_t tests_count);

    void onChangedTestName(String old_test_name); // String is passed by value as it's swapped with _test_.

private:
    Writer();

    const Poco::Logger * base_log {nullptr}; // Initialized when server initializes Poco internal structures.

    const SymbolIndexInstance symbol_index;
    const Dwarf dwarf;

    // CH client is usually located inside main CH binary, but we don't need to instrument client code.
    // This variable is set on client initialization so we can ignore coverage for it.
    bool is_client {false};

    TaskQueue tasks_queue;

    std::vector<SourceInfo> source_files_cache;

    std::vector<BBInfo> bb_cache;

    std::vector<TestData> tests; /// Data accumulated for all tests.

    std::span<bool> test_data; /// Counters for currently active test
    size_t bb_count; // cached test_data size

    size_t test_index {0}; /// Index for test that will run next.

    FileWrapper report_file;

    void deinitRuntime();

    void prepareDataAndDump(TestData& test_data, const EdgesHit& hits);

    void writeCCRHeader();
    void writeCCREntry(const TestData& test_data);
    void writeCCRFooter();

    void symbolizeInstrumentedData();

    using SourceSymbolizedData = std::unordered_map<BBIndex, Line>;
    using LocalCache = std::unordered_map<SourcePath, SourceSymbolizedData>;
    using LocalCaches = std::vector<LocalCache>;

    void symbolizeAddrsIntoLocalCaches(LocalCaches& caches);
    void mergeIntoGlobalCache(const LocalCaches& caches);
};
}
