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
 *   echo 'SELECT 1;' | ./parser_memory_profiler
 *   # Output: 9    2437760    2441632    3872
 *   # Format: query_length \t before \t after \t diff (bytes)
 *
 *   # Complex query
 *   echo 'SELECT a, b, c FROM t1 JOIN t2 ON t1.id = t2.id WHERE x > 10;' | ./parser_memory_profiler
 *
 *
 * WITH HEAP PROFILING (generates .heap files for jeprof analysis)
 * ---------------------------------------------------------------
 *
 *   # On macOS (jemalloc uses je_ prefix):
 *   JE_MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:0 \
 *       ./parser_memory_profiler --profile /tmp/query_ <<< 'SELECT 1;'
 *
 *   # On Linux:
 *   MALLOC_CONF=prof:true,lg_prof_sample:0 \
 *       ./parser_memory_profiler --profile /tmp/query_ <<< 'SELECT 1;'
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
 *       ./parser_memory_profiler \
 *       /tmp/query_after.*.heap
 *
 *   # Generate SVG call graph:
 *   jeprof --svg --show_bytes \
 *       --base=/tmp/query_before.*.heap \
 *       ./parser_memory_profiler \
 *       /tmp/query_after.*.heap > /tmp/parser.svg
 *
 *   # Generate flame graph:
 *   jeprof --collapsed --show_bytes \
 *       --base=/tmp/query_before.*.heap \
 *       ./parser_memory_profiler \
 *       /tmp/query_after.*.heap | \
 *       flamegraph.pl --title "Parser Memory" > /tmp/flame.svg
 *
 *
 * BATCH PROCESSING
 * ----------------
 *
 *   # Process multiple queries from a file:
 *   while IFS= read -r query; do
 *       echo "$query" | ./parser_memory_profiler
 *   done < queries.txt
 *
 *   # See run_profiler.sh and generate_report.py for full batch processing
 *   # with HTML report generation.
 */

#include <iostream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <unistd.h>
#include <atomic>

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
    int ret = mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    if (ret != 0)
        std::cerr << "Warning: thread.tcache.flush failed: " << ret << "\n";
}

/// Refresh jemalloc stats epoch
void refreshJemallocEpoch()
{
    uint64_t epoch = 1;
    size_t epoch_size = sizeof(epoch);
    int ret = mallctl("epoch", &epoch, &epoch_size, &epoch, epoch_size);
    if (ret != 0)
        std::cerr << "Warning: epoch refresh failed: " << ret << "\n";
}

/// Get current allocated bytes from jemalloc
size_t getJemallocAllocated()
{
    size_t allocated = 0;
    size_t allocated_size = sizeof(allocated);
    int ret = mallctl("stats.allocated", &allocated, &allocated_size, nullptr, 0);
    if (ret != 0)
        std::cerr << "Warning: stats.allocated failed: " << ret << "\n";
    return allocated;
}

/// Check if jemalloc was compiled with profiling support
bool isProfilingCompiled()
{
    bool compiled = false;
    size_t sz = sizeof(compiled);
    int ret = mallctl("config.prof", &compiled, &sz, nullptr, 0);
    return (ret == 0) && compiled;
}

/// Check if jemalloc profiling is enabled at runtime
bool isProfilingEnabled()
{
    bool enabled = false;
    size_t enabled_size = sizeof(enabled);
    int ret = mallctl("opt.prof", &enabled, &enabled_size, nullptr, 0);
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
    int ret = mallctl("prof.active", nullptr, nullptr, &active, sizeof(active));
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
    int ret = mallctl("prof.reset", nullptr, nullptr, nullptr, 0);
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
    int ret = mallctl("prof.dump", nullptr, nullptr, &path_ptr, sizeof(path_ptr));
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

void printUsage(const char * prog_name)
{
    std::cerr << "Usage: " << prog_name << " [--profile <prefix>] [--symbolize]\n";
    std::cerr << "  Reads SQL query from stdin until EOF and prints memory stats.\n";
    std::cerr << "\nOptions:\n";
    std::cerr << "  --profile <prefix>  Dump jemalloc heap profiles to <prefix>*.heap\n";
    std::cerr << "  --symbolize         Symbolize heap profiles (requires --profile)\n";
    std::cerr << "\nOutput format (tab-separated):\n";
    std::cerr << "  query_length  allocated_before  allocated_after  allocated_diff\n";
    std::cerr << "\nExamples:\n";
    std::cerr << "  # Memory stats only:\n";
    std::cerr << "  echo 'SELECT 1;' | " << prog_name << "\n";
    std::cerr << "\n  # With heap profiling and symbolization (Linux):\n";
    std::cerr << "  MALLOC_CONF=prof:true,lg_prof_sample:0 " << prog_name << " --profile /tmp/p_ --symbolize <<< 'SELECT 1;'\n";
}

void printProfilingStatus()
{
    std::cerr << "Jemalloc profiling status:\n";

    bool prof_compiled = false;
    size_t sz = sizeof(prof_compiled);
    int ret = mallctl("config.prof", &prof_compiled, &sz, nullptr, 0);
    std::cerr << "  config.prof (compiled with profiling): " << (ret == 0 ? (prof_compiled ? "yes" : "no") : "error") << "\n";

    bool prof_enabled = false;
    sz = sizeof(prof_enabled);
    ret = mallctl("opt.prof", &prof_enabled, &sz, nullptr, 0);
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
        std::cerr << "\nOn macOS (jemalloc with je_ prefix), use JE_MALLOC_CONF:\n";
        std::cerr << "  JE_MALLOC_CONF=prof:true,prof_active:true <program> --profile <prefix>\n";
        std::cerr << "\nOn Linux (jemalloc without prefix), use MALLOC_CONF:\n";
        std::cerr << "  MALLOC_CONF=prof:true <program> --profile <prefix>\n";
    }
}

}


int main(int argc, char ** argv)
try
{
    using namespace DB;

    std::string profile_prefix;
    bool enable_profiling = false;
    bool enable_symbolize = false;

    for (int i = 1; i < argc; ++i)
    {
        std::string arg = argv[i];
        if (arg == "--profile" && i + 1 < argc)
        {
            profile_prefix = argv[++i];
            enable_profiling = true;
        }
        else if (arg == "--symbolize")
        {
            enable_symbolize = true;
        }
        else if (arg == "--help" || arg == "-h")
        {
            printUsage(argv[0]);
            return 0;
        }
        else
        {
            std::cerr << "Unknown option: " << arg << "\n";
            printUsage(argv[0]);
            return 1;
        }
    }

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
        mallctl("thread.prof.active", nullptr, nullptr, &thread_active, sizeof(thread_active));

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

