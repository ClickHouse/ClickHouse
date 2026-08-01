/**
 * Parser Memory Profiler
 * ======================
 *
 * A tool to measure memory allocations during SQL query parsing.
 * Useful for analyzing parser memory usage patterns and identifying
 * queries that allocate excessive memory during AST construction.
 *
 *
 * BASIC USAGE - Memory Stats Only
 * -------------------------------
 *
 *   # Simple query
 *   echo 'SELECT 1;' | ./clickhouse-examples parser_memory_profiler
 *   # Output: 9    2437760    2441632    3872
 *   # Format: query_length \t before \t after \t diff (bytes)
 *
 *   # Complex query
 *   echo 'SELECT a, b, c FROM t1 JOIN t2 ON t1.id = t2.id WHERE x > 10;' | ./clickhouse-examples parser_memory_profiler
 *
 *
 * WITH HEAP PROFILING (generates .heap files for jeprof analysis)
 * ---------------------------------------------------------------
 *
 *   MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:0 \
 *       ./clickhouse-examples parser_memory_profiler --profile /tmp/query_ <<< 'SELECT 1;'
 *
 *   # This generates:
 *   #   /tmp/query_before.<pid>.0.heap  - heap state before parsing
 *   #   /tmp/query_after.<pid>.1.heap   - heap state after parsing
 *
 *
 * ANALYZING HEAP PROFILES WITH JEPROF
 * -----------------------------------
 *
 *   # Text report showing allocation diff:
 *   jeprof --text --show_bytes \
 *       --base=/tmp/query_before.*.heap \
 *       ./clickhouse-examples \
 *       /tmp/query_after.*.heap
 *
 *   # Generate SVG call graph:
 *   jeprof --svg --show_bytes \
 *       --base=/tmp/query_before.*.heap \
 *       ./clickhouse-examples \
 *       /tmp/query_after.*.heap > /tmp/parser.svg
 *
 *   # Generate flame graph:
 *   jeprof --collapsed --show_bytes \
 *       --base=/tmp/query_before.*.heap \
 *       ./clickhouse-examples \
 *       /tmp/query_after.*.heap | \
 *       flamegraph.pl --title "Parser Memory" > /tmp/flame.svg
 *
 *
 * BATCH PROCESSING
 * ----------------
 *
 *   # Process multiple queries from a file:
 *   while IFS= read -r query; do
 *       echo "$query" | ./clickhouse-examples parser_memory_profiler
 *   done < queries.txt
 *
 *   # See run_profiler.sh and generate_report.py for full batch processing
 *   # with HTML report generation.
 */

#include <iostream>
#include <string>
#include <cstdlib>
#include <filesystem>
#include <unistd.h>
#include <atomic>

#include <boost/program_options.hpp>

#include <Common/Exception.h>
#include <Common/Jemalloc.h>
#include <Processors/Sources/JemallocProfileSource.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <jemalloc/jemalloc.h>


namespace
{

/// Flush jemalloc thread cache to get accurate global stats
void flushJemallocThreadCache()
{
    int ret = je_mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    if (ret != 0)
        std::cerr << "Warning: thread.tcache.flush failed: " << ret << "\n";
}

/// Refresh jemalloc stats epoch
void refreshJemallocEpoch()
{
    uint64_t epoch = 1;
    size_t epoch_size = sizeof(epoch);
    int ret = je_mallctl("epoch", &epoch, &epoch_size, &epoch, epoch_size);
    if (ret != 0)
        std::cerr << "Warning: epoch refresh failed: " << ret << "\n";
}

/// Get current allocated bytes from jemalloc
size_t getJemallocAllocated()
{
    size_t allocated = 0;
    size_t allocated_size = sizeof(allocated);
    int ret = je_mallctl("stats.allocated", &allocated, &allocated_size, nullptr, 0);
    if (ret != 0)
        std::cerr << "Warning: stats.allocated failed: " << ret << "\n";
    return allocated;
}

/// Check if jemalloc was compiled with profiling support
bool isProfilingCompiled()
{
    bool compiled = false;
    size_t sz = sizeof(compiled);
    int ret = je_mallctl("config.prof", &compiled, &sz, nullptr, 0);
    return (ret == 0) && compiled;
}

/// Check if jemalloc profiling is enabled at runtime
bool isProfilingEnabled()
{
    bool enabled = false;
    size_t enabled_size = sizeof(enabled);
    int ret = je_mallctl("opt.prof", &enabled, &enabled_size, nullptr, 0);
    if (ret != 0)
    {
        std::cerr << "Warning: opt.prof query failed: " << ret << "\n";
        return false;
    }
    return enabled;
}

/// Try to enable profiling at runtime (may not work if not enabled at startup)
bool tryEnableProfiling()
{
    /// First check if it's already enabled
    if (isProfilingEnabled())
        return true;

    /// Try to enable prof.active
    bool active = true;
    int ret = je_mallctl("prof.active", nullptr, nullptr, &active, sizeof(active));
    if (ret != 0)
    {
        std::cerr << "Note: Cannot enable prof.active at runtime (ret=" << ret << ")\n";
        std::cerr << "      Profiling must be enabled at startup via MALLOC_CONF=prof:true\n";
        std::cerr << "      On macOS, try: MALLOC_CONF=prof:true,prof_active:true <program>\n";
        return false;
    }
    return true;
}

/// Reset jemalloc profiler counters
bool resetProfiler()
{
    int ret = je_mallctl("prof.reset", nullptr, nullptr, nullptr, 0);
    if (ret != 0)
    {
        std::cerr << "Warning: prof.reset failed: " << ret << "\n";
        return false;
    }
    return true;
}

/// Dump jemalloc heap profile to file
std::string dumpProfile(const std::string & prefix)
{
    static std::atomic<size_t> counter{0};
    std::string path = prefix + std::to_string(getpid()) + "." + std::to_string(counter.fetch_add(1)) + ".heap";
    const char * path_ptr = path.c_str();
    int ret = je_mallctl("prof.dump", nullptr, nullptr, &path_ptr, sizeof(path_ptr));
    if (ret != 0)
    {
        std::cerr << "Error: prof.dump failed: " << ret << " (path: " << path << ")\n";
        return "";
    }
    return path;
}

/// Read input until EOF
std::string readQuery()
{
    return std::string(
        std::istreambuf_iterator<char>(std::cin),
        std::istreambuf_iterator<char>()
    );
}

namespace po = boost::program_options;

void printProfilingStatus()
{
    std::cerr << "Jemalloc profiling status:\n";

    bool prof_compiled = false;
    size_t sz = sizeof(prof_compiled);
    int ret = je_mallctl("config.prof", &prof_compiled, &sz, nullptr, 0);
    std::cerr << "  config.prof (compiled with profiling): " << (ret == 0 ? (prof_compiled ? "yes" : "no") : "error") << "\n";

    bool prof_enabled = false;
    sz = sizeof(prof_enabled);
    ret = je_mallctl("opt.prof", &prof_enabled, &sz, nullptr, 0);
    std::cerr << "  opt.prof (profiling enabled at runtime): " << (ret == 0 ? (prof_enabled ? "yes" : "no") : "error") << "\n";

    if (!prof_compiled)
    {
        std::cerr << "\nProfiling is not available. jemalloc was compiled without JEMALLOC_PROF.\n";
        std::cerr << "On Linux builds, profiling should be available.\n";
        std::cerr << "On macOS, profiling support is limited.\n";
    }
    else if (!prof_enabled)
    {
        std::cerr << "\nProfiler is compiled but not enabled. This must be set at startup.\n";
        std::cerr << "\nUse MALLOC_CONF to enable profiling:\n";
        std::cerr << "  MALLOC_CONF=prof:true,prof_active:true <program> --profile <prefix>\n";
    }
}

}


int mainEntryExampleParserMemoryProfiler(int argc, char ** argv)
try
{
    using namespace DB;

    po::options_description desc("Measure memory allocations during SQL parsing.\n"
                                 "Reads query from stdin, prints tab-separated: query_length before after diff\n\n"
                                 "Options");
    desc.add_options()
        ("help,h", "Show help")
        ("profile", po::value<std::string>(), "Dump jemalloc heap profiles with this prefix")
        ("symbolize", "Symbolize heap profiles (requires --profile)")
        ("symbolize-batch", po::value<std::vector<std::string>>()->multitoken(),
         "Batch-symbolize .heap files (shared cache via global LRU)")
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.contains("help"))
    {
        std::cerr << desc << "\n";
        return 0;
    }

    /// Handle batch symbolization mode: global LRU cache is shared across calls
    if (vm.contains("symbolize-batch"))
    {
        auto batch_files = vm["symbolize-batch"].as<std::vector<std::string>>();
        if (batch_files.empty())
        {
            std::cerr << "Error: --symbolize-batch requires at least one .heap file\n";
            return 1;
        }

        for (const auto & file : batch_files)
        {
            if (!std::filesystem::exists(file))
            {
                std::cerr << "Error: heap file not found: " << file << "\n";
                return 1;
            }
        }

        std::cerr << "Batch symbolizing " << batch_files.size() << " heap files...\n";
        for (const auto & file : batch_files)
        {
            std::string output = file + ".sym";
            symbolizeJemallocHeapProfile(file, output);
            std::cerr << "Symbolized: " << file << " -> " << output << "\n";
        }
        std::cerr << "Done.\n";
        return 0;
    }

    std::string profile_prefix;
    bool enable_profiling = vm.contains("profile");
    bool enable_symbolize = vm.contains("symbolize");

    if (enable_profiling)
        profile_prefix = vm["profile"].as<std::string>();

    if (enable_symbolize && !enable_profiling)
    {
        std::cerr << "Error: --symbolize requires --profile\n";
        return 1;
    }

    std::string query = readQuery();

    if (query.empty())
    {
        std::cerr << "Error: empty query\n";
        return 1;
    }

    size_t query_length = query.size();
    std::string profile_before_path;
    std::string profile_after_path;

    if (enable_profiling)
    {
        if (!isProfilingCompiled())
        {
            std::cerr << "Error: jemalloc was not compiled with profiling support.\n\n";
            printProfilingStatus();
            return 1;
        }

        if (!isProfilingEnabled())
        {
            std::cerr << "Warning: jemalloc profiling not enabled at startup.\n";
            if (!tryEnableProfiling())
            {
                std::cerr << "\nError: Cannot enable profiling.\n\n";
                printProfilingStatus();
                return 1;
            }
        }

        /// Enable per-thread profiling (ClickHouse jemalloc defaults to prof_thread_active_init:false)
        bool thread_active = true;
        je_mallctl("thread.prof.active", nullptr, nullptr, &thread_active, sizeof(thread_active));

        /// Reset profiler and dump initial state
        resetProfiler();
        profile_before_path = dumpProfile(profile_prefix + "before.");
        if (profile_before_path.empty())
            return 1;
    }

    /// Flush tcache and refresh epoch before measuring
    flushJemallocThreadCache();
    refreshJemallocEpoch();
    size_t allocated_before = getJemallocAllocated();

    ParserQuery parser(query.data() + query.size());
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, 0, 0);

    /// Flush tcache and refresh epoch after parsing to capture all allocations
    flushJemallocThreadCache();
    refreshJemallocEpoch();
    size_t allocated_after = getJemallocAllocated();

    if (enable_profiling)
    {
        profile_after_path = dumpProfile(profile_prefix + "after.");
        if (profile_after_path.empty())
            return 1;
    }

    /// Machine-readable output: tab-separated values
    /// query_length \t allocated_before \t allocated_after \t allocated_diff
    std::cout << query_length << "\t"
              << allocated_before << "\t"
              << allocated_after << "\t"
              << (allocated_after > allocated_before ? allocated_after - allocated_before : 0)
              << "\n";

    if (enable_profiling)
    {
        std::cerr << "Profile before: " << profile_before_path << "\n";
        std::cerr << "Profile after:  " << profile_after_path << "\n";

        if (enable_symbolize)
        {
            std::string sym_before = profile_before_path + ".sym";
            std::string sym_after = profile_after_path + ".sym";
            symbolizeJemallocHeapProfile(profile_before_path, sym_before);
            symbolizeJemallocHeapProfile(profile_after_path, sym_after);
            std::cerr << "Symbolized before: " << sym_before << "\n";
            std::cerr << "Symbolized after:  " << sym_after << "\n";
        }
    }

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    return 1;
}

