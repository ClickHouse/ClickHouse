#if defined(__ELF__) && !defined(OS_FREEBSD) && WITH_COVERAGE_DEPTH

#include <Common/CoverageCollection.h>
#include <Common/LLVMCoverageMapping.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Defines.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <base/coverage.h>

#include <fcntl.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Key for the coverage map: (NameRef, FuncHash, CounterId) triple.
/// The counter_id identifies a specific basic-block region within the function,
/// giving statement-level granularity instead of function-level.
struct CoverageKey
{
    uint64_t name_hash;
    uint64_t func_hash;
    uint32_t counter_id;

    bool operator==(const CoverageKey & o) const
    {
        return name_hash == o.name_hash && func_hash == o.func_hash && counter_id == o.counter_id;
    }
};

struct CoverageKeyHash
{
    std::size_t operator()(const CoverageKey & k) const
    {
        std::size_t h = k.name_hash ^ (k.func_hash * 0x9e3779b97f4a7c15ULL);
        h ^= static_cast<std::size_t>(k.counter_id) * 0x517cc1b727220a95ULL;
        return h;
    }
};

/// Lazily-loaded map from (NameRef, FuncHash, CounterId) → CoverageRegion.
/// When multiple regions share the same counter_id within a function, the
/// narrowest one (smallest line range) is kept for maximum precision.
std::unordered_map<CoverageKey, CoverageRegion, CoverageKeyHash> g_coverage_map;
std::once_flag g_coverage_map_once;

void ensureCoverageMapLoaded()
{
    std::call_once(g_coverage_map_once, []
    {
        const auto regions = readLLVMCoverageMapping("/proc/self/exe");
        g_coverage_map.reserve(regions.size());
        for (const CoverageRegion & r : regions)
        {
            const CoverageKey key{r.name_hash, r.func_hash, r.counter_id};
            auto [it, inserted] = g_coverage_map.emplace(key, r);
            if (!inserted)
            {
                /// Keep the narrowest region for this counter.  Prefer branch regions
                /// over code regions of the same width — they carry directional info.
                const uint32_t existing_width = it->second.line_end - it->second.line_start;
                const uint32_t new_width      = r.line_end - r.line_start;
                if (new_width < existing_width
                    || (new_width == existing_width && r.is_branch && !it->second.is_branch))
                    it->second = r;
            }
        }

        LOG_INFO(
            getLogger("CoverageCollection"),
            "Loaded {} counter regions from LLVM coverage mapping ({} raw regions)",
            g_coverage_map.size(), regions.size());
    });
}

} // anonymous namespace


ResolvedCoverage resolveCoverage(const std::vector<CovCounter> & name_refs)
{
    ensureCoverageMapLoaded();

    /// Collect unique (file, line_start, line_end) triples.
    struct LineKey
    {
        std::string file;
        uint32_t line_start;
        uint32_t line_end;

        bool operator==(const LineKey & o) const
        {
            return line_start == o.line_start && line_end == o.line_end && file == o.file;
        }
    };
    struct LineKeyHash
    {
        std::size_t operator()(const LineKey & k) const
        {
            std::size_t h = std::hash<std::string>{}(k.file);
            h ^= std::hash<uint32_t>{}(k.line_start) + 0x9e3779b9u + (h << 6) + (h >> 2);
            h ^= std::hash<uint32_t>{}(k.line_end)   + 0x9e3779b9u + (h << 6) + (h >> 2);
            return h;
        }
    };

    /// Per (file, line_start, line_end) key: output array index + min depth + branch flag.
    struct SeenEntry { size_t idx; uint8_t min_depth; uint8_t branch_flag; };
    std::unordered_map<LineKey, SeenEntry, LineKeyHash> seen;
    seen.reserve(name_refs.size());

    std::vector<std::string> files;
    std::vector<uint32_t> line_starts;
    std::vector<uint32_t> line_ends;
    std::vector<uint8_t> min_depths;
    /// branch_flags: 0 = code region, 1 = true branch, 2 = false branch.
    std::vector<uint8_t> branch_flags;

    for (const auto & [name_hash, func_hash, counter_id, min_depth] : name_refs)
    {
        const auto it = g_coverage_map.find(CoverageKey{name_hash, func_hash, counter_id});
        if (it == g_coverage_map.end())
            continue;

        const CoverageRegion & region = it->second;
        if (region.file.empty() || region.line_start == 0)
            continue;

        const uint8_t bflag = region.is_branch ? (region.is_true_branch ? 1u : 2u) : 0u;

        LineKey key{region.file, region.line_start, region.line_end};
        const auto [sit, inserted] = seen.emplace(key, SeenEntry{files.size(), min_depth, bflag});
        if (inserted)
        {
            files.push_back(region.file);
            line_starts.push_back(region.line_start);
            line_ends.push_back(region.line_end);
            min_depths.push_back(min_depth);
            branch_flags.push_back(bflag);
        }
        else if (min_depth < sit->second.min_depth)
        {
            /// Keep branch_flag in sync with the min_depth record: they must describe
            /// the same region.  Without this, branch_flag would belong to whichever
            /// region was inserted first, while min_depth came from a different region.
            sit->second.min_depth = min_depth;
            sit->second.branch_flag = bflag;
            min_depths[sit->second.idx] = min_depth;
            branch_flags[sit->second.idx] = bflag;
        }
    }

    return ResolvedCoverage{
        .files = std::move(files),
        .line_starts = std::move(line_starts),
        .line_ends = std::move(line_ends),
        .min_depths = std::move(min_depths),
        .branch_flags = std::move(branch_flags)};
}

size_t getCoverageMapSize()
{
    ensureCoverageMapLoaded();
    return g_coverage_map.size();
}

size_t countCoverageMatches(const std::vector<CovCounter> & name_refs)
{
    ensureCoverageMapLoaded();
    size_t count = 0;
    for (const auto & [name_hash, func_hash, counter_id, min_depth] : name_refs)
        if (g_coverage_map.count(CoverageKey{name_hash, func_hash, counter_id}))
            ++count;
    return count;
}

uint64_t getFirstCoverageMapKey()
{
    ensureCoverageMapLoaded();
    if (g_coverage_map.empty())
        return 0;
    return g_coverage_map.begin()->first.name_hash;
}

/// Returns (non_empty_file_count, zero_line_count, first_file_hash)
/// among matched regions for diagnostic purposes.
std::tuple<size_t, size_t, uint64_t> diagCoverageRegions(const std::vector<CovCounter> & name_refs)
{
    ensureCoverageMapLoaded();
    size_t non_empty = 0, zero_line = 0;
    uint64_t first_file_hash = 0;
    for (const auto & [name_hash, func_hash, counter_id, min_depth] : name_refs)
    {
        auto it = g_coverage_map.find(CoverageKey{name_hash, func_hash, counter_id});
        if (it == g_coverage_map.end()) continue;
        const CoverageRegion & r = it->second;
        if (!r.file.empty())
        {
            ++non_empty;
            if (first_file_hash == 0)
            {
                /// Store length of first file as a proxy diagnostic
                first_file_hash = static_cast<uint64_t>(r.file.size()) << 32
                    | static_cast<uint64_t>(r.line_start);
            }
        }
        if (r.line_start == 0) ++zero_line;
    }
    return {non_empty, zero_line, first_file_hash};
}

CurrentCoverageRegions getCurrentCoverageRegions()
{
    ensureCoverageMapLoaded();

    auto name_refs = getCurrentCoveredNameRefs();

    /// Collect unique (file, line_start, line_end) triples — same dedup logic as collectAndInsertCoverage.
    struct LineKey
    {
        std::string file;
        uint32_t line_start;
        uint32_t line_end;

        bool operator==(const LineKey & o) const
        {
            return line_start == o.line_start && line_end == o.line_end && file == o.file;
        }
    };
    struct LineKeyHash
    {
        std::size_t operator()(const LineKey & k) const
        {
            std::size_t h = std::hash<std::string>{}(k.file);
            h ^= std::hash<uint32_t>{}(k.line_start) + 0x9e3779b9u + (h << 6) + (h >> 2);
            h ^= std::hash<uint32_t>{}(k.line_end)   + 0x9e3779b9u + (h << 6) + (h >> 2);
            return h;
        }
    };

    std::unordered_map<LineKey, bool, LineKeyHash> seen;
    seen.reserve(name_refs.size());

    CurrentCoverageRegions out;
    for (const auto & [name_hash, func_hash, counter_id, min_depth] : name_refs)
    {
        const auto it = g_coverage_map.find(CoverageKey{name_hash, func_hash, counter_id});
        if (it == g_coverage_map.end())
            continue;

        const CoverageRegion & region = it->second;
        if (region.file.empty() || region.line_start == 0)
            continue;

        LineKey key{region.file, region.line_start, region.line_end};
        if (!seen.emplace(key, true).second)
            continue;

        out.files.push_back(region.file);
        out.line_starts.push_back(region.line_start);
        out.line_ends.push_back(region.line_end);
    }

    return out;
}


namespace
{

void reportToStderr(const std::string & message)
{
    WriteBufferFromFileDescriptor err(STDERR_FILENO);
    writeString(message, err);
    writeChar('\n', err);
    err.finalize();
}

/// Set once by `initCoverageFromEnvironment`, before any flush.
std::string g_coverage_file_prefix;

/// Append the rows in the layout of `system.coverage_log` and `system.coverage_indirect_calls`
/// (without `time`) to `<prefix>.lines.tsv` and `<prefix>.indirect_calls.tsv`.
void writeCoverageToFiles(
    std::string_view test_name,
    const std::vector<CovCounter> & name_refs,
    const std::vector<IndirectCallEntry> & indirect_calls)
{
    const ResolvedCoverage resolved = resolveCoverage(name_refs);
    {
        WriteBufferFromFile out(g_coverage_file_prefix + ".lines.tsv", DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
        for (size_t i = 0; i < resolved.files.size(); ++i)
        {
            writeEscapedString(test_name, out);
            writeChar('\t', out);
            writeEscapedString(resolved.files[i], out);
            writeChar('\t', out);
            writeIntText(resolved.line_starts[i], out);
            writeChar('\t', out);
            writeIntText(resolved.line_ends[i], out);
            writeChar('\t', out);
            writeIntText(static_cast<UInt32>(resolved.min_depths[i]), out);
            writeChar('\t', out);
            writeIntText(static_cast<UInt32>(resolved.branch_flags[i]), out);
            writeChar('\n', out);
        }
        out.finalize();
    }
    {
        WriteBufferFromFile out(g_coverage_file_prefix + ".indirect_calls.tsv", DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
        for (const auto & call : indirect_calls)
        {
            writeEscapedString(test_name, out);
            writeChar('\t', out);
            writeIntText(call.caller_name_hash, out);
            writeChar('\t', out);
            writeIntText(call.caller_func_hash, out);
            writeChar('\t', out);
            writeIntText(call.callee_offset, out);
            writeChar('\t', out);
            writeIntText(call.call_count, out);
            writeChar('\n', out);
        }
        out.finalize();
    }
}

}

void flushCoverageToFilesOnExit() noexcept
{
    if (g_coverage_file_prefix.empty())
        return;
    try
    {
        setCoverageTest("");
    }
    catch (...)
    {
        /// The process is exiting or crashing: there is nobody to propagate the error to.
        try
        {
            reportToStderr("Cannot flush per-test coverage: " + getCurrentExceptionMessage(false));
        }
        catch (...) // NOLINT(bugprone-empty-catch): stderr is the last resort, the process is exiting anyway
        {
        }
    }
}

bool isCoverageFileSinkEnabled()
{
    return !g_coverage_file_prefix.empty();
}

void initCoverageFromEnvironment(const std::string & log_path)
{
    const char * test_name = std::getenv("CLICKHOUSE_COVERAGE_TEST_NAME"); // NOLINT(concurrency-mt-unsafe)
    if (!test_name || !*test_name)
        return;
    if (log_path.empty())
    {
        /// E.g. a bridge started without a log file. Refusing to start would break the test
        /// for the sake of its coverage, so this process only reports that it is not collected.
        reportToStderr("CLICKHOUSE_COVERAGE_TEST_NAME is set, but there is no `logger.log` to put the coverage next to; "
            "the coverage of this process is not collected");
        return;
    }

    const auto directory = std::filesystem::path(log_path).parent_path() / "coverage";
    std::filesystem::create_directories(directory);
    g_coverage_file_prefix = directory / std::to_string(getpid());

    registerCoverageFlushCallback(writeCoverageToFiles);
    setCoverageDumpHook(flushCoverageToFilesOnExit);
    if (0 != std::atexit([] { flushCoverageToFilesOnExit(); }))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot register the per-test coverage flush at exit");
    setCoverageTest(test_name);
}

}

#endif
