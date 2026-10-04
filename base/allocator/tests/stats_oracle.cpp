/// Compares the structure of the statistics output (`statsPrint`, jemalloc's `stats_print`) with the reference
/// jemalloc's `je_malloc_stats_print` in the same process, for every omission flag: keys, their order, the table
/// layout (column widths, headers, gap markers of the fixed parts). The values differ (the two allocators have
/// different histories), so numbers are masked; the exact values are checked by the differential driver
/// (tests/diff, section `stats_print`).

#include <allocator/Ctl.h>
#include <allocator/Frontend.h>
#include <allocator/Stats.h>
#include <allocator/ThreadState.h>

#include "Test.h"

#include <cerrno>
#include <sched.h>
#include <string>
#include <vector>

extern "C"
{
void je_malloc_stats_print(void (*write_cb)(void *, const char *), void * cbopaque, const char * opts);

/// The reference pulls in the libunwind-based profiler backtrace, which is never called here.
int unw_backtrace(void **, int)
{
    return 0;
}
}

namespace
{

void appendCallback(void * opaque, const char * s)
{
    static_cast<std::string *>(opaque)->append(s);
}

bool isDigit(char c)
{
    return c >= '0' && c <= '9';
}

/// Replaces every run of digits with `N`.
std::string maskNumbers(const std::string & s)
{
    std::string result;
    for (size_t i = 0; i < s.size();)
    {
        if (isDigit(s[i]))
        {
            result += 'N';
            while (i < s.size() && isDigit(s[i]))
                ++i;
        }
        else
        {
            result += s[i++];
        }
    }
    return result;
}

/// Collapses runs of spaces into one space.
std::string collapseSpaces(const std::string & s)
{
    std::string result;
    for (char c : s)
        if (!(c == ' ' && !result.empty() && result.back() == ' '))
            result += c;
    return result;
}

/// The table output, line by line. Rows of the per-size-class tables (bins, large, extents, nonfull slabs) and their
/// gap markers depend on which size classes are in use, so they are dropped. In tabular lines (with column padding)
/// the numbers have right-justified fixed-width columns: the line length is kept, the padding is collapsed.
std::vector<std::string> normalizeTable(const std::string & text)
{
    std::vector<std::string> lines;
    size_t begin = 0;
    while (begin < text.size())
    {
        size_t end = text.find('\n', begin);
        if (end == std::string::npos)
            end = text.size();
        std::string line = text.substr(begin, end - begin);
        begin = end + 1;

        size_t first = line.find_first_not_of(' ');
        if (first != std::string::npos && first > 0 && (isDigit(line[first]) || line.compare(first, std::string::npos, "---") == 0))
            continue;

        if (line.find("  ") != std::string::npos)
            lines.push_back(std::to_string(line.size()) + "|" + collapseSpaces(maskNumbers(line)));
        else
            lines.push_back(maskNumbers(line));
    }
    return lines;
}

/// The (compact, single-line) JSON output split into tokens at the structural characters.
std::vector<std::string> normalizeJson(const std::string & text)
{
    std::vector<std::string> tokens;
    std::string current;
    for (char c : maskNumbers(text))
    {
        current += c;
        if (c == ',' || c == '{' || c == '}' || c == '[' || c == ']')
        {
            tokens.push_back(current);
            current.clear();
        }
    }
    tokens.push_back(current);
    return tokens;
}

void compareLines(const std::vector<std::string> & ref, const std::vector<std::string> & ours, const char * opts)
{
    size_t n = std::min(ref.size(), ours.size());
    for (size_t i = 0; i < n; ++i)
    {
        if (ref[i] != ours[i])
        {
            std::fprintf(stderr, "opts \"%s\": difference at item %zu:\n  ref: %s\n  new: %s\n", opts, i, ref[i].c_str(), ours[i].c_str());
            CHECK(false);
            return;
        }
    }
    CHECK_EQ(ref.size(), ours.size());
}

void compareOutputs(const char * opts)
{
    std::string ref;
    je_malloc_stats_print(&appendCallback, &ref, opts);
    std::string ours;
    jemalloc::statsPrint(&appendCallback, &ours, opts);

    CHECK(!ref.empty());
    bool json = std::string(opts).find('J') != std::string::npos;
    if (json)
    {
        CHECK(ours.back() == '}');
        compareLines(normalizeJson(ref), normalizeJson(ours), opts);
    }
    else
    {
        CHECK(ours.ends_with("--- End jemalloc statistics ---\n"));
        compareLines(normalizeTable(ref), normalizeTable(ours), opts);
    }
}

/// Whether the leaves read by the general section exist (the profiling ones are implemented by the Prof module).
bool generalLeavesAvailable()
{
    REQUIRE(!jemalloc::mallocInit());
    for (const char * name : {"prof.thread_active_init", "prof.active", "prof.gdump", "prof.interval", "prof.lg_sample"})
    {
        bool value[8];
        size_t size = sizeof(value);
        if (jemalloc::ctlByName(jemalloc::ThreadState::fetch(), name, value, &size, nullptr, 0) == ENOENT)
        {
            std::fprintf(stderr, "SKIPPED the general section: the leaf %s does not exist yet\n", name);
            return false;
        }
    }
    return true;
}

/// Both allocators place the main thread on the arena of the same CPU (per-CPU arenas). Called after the
/// initialization of both (the per-CPU mode is disabled if the affinity mask differs from the number of CPUs).
void pinToCpu0()
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    REQUIRE(sched_setaffinity(0, sizeof(set), &set) == 0);
}

}

TEST(StatsOracle, Structure)
{
    bool general = generalLeavesAvailable();
    pinToCpu0();

    /// Every flag separately and some combinations, in both output modes.
    static const char * const options[] = {
        "", "g", "m", "d", "a", "b", "l", "x", "e", "h", "gbla", "gmdablxeh", "gmdablxehq", "ga", "gm", "gxbleh", "bbb",
    };
    for (const char * table_opts : options)
    {
        std::string opts_table = table_opts;
        std::string opts_json = std::string("J") + table_opts;
        if (!general && opts_table.find('g') == std::string::npos)
            continue;
        compareOutputs(opts_table.c_str());
        compareOutputs(opts_json.c_str());
    }
    /// A null `opts` is the default.
    if (general)
    {
        std::string ref;
        je_malloc_stats_print(&appendCallback, &ref, nullptr);
        std::string ours;
        jemalloc::statsPrint(&appendCallback, &ours, nullptr);
        compareLines(normalizeTable(ref), normalizeTable(ours), "(null)");
    }
}
