#if defined(__ELF__) && !defined(OS_FREEBSD) && WITH_COVERAGE_DEPTH

#include <Common/CoverageCollection.h>
#include <Common/LLVMCoverageMapping.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <QueryPipeline/BlockIO.h>
#include <base/coverage.h>

#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>


namespace DB
{

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


void collectAndInsertCoverage(
    std::string_view test_name,
    const std::vector<CovCounter> & name_refs,
    const std::vector<IndirectCallEntry> & indirect_calls,
    ContextPtr context)
{
    if (name_refs.empty())
    {
        auto msg = fmt::format("CoverageCollection: No covered counters for test '{}', skipping", test_name);
        LOG_INFO(getLogger("CoverageCollection"), "{}", msg);
        return;
    }

    ensureCoverageMapLoaded();

    LOG_INFO(getLogger("CoverageCollection"),
        "Flushing test '{}': {} covered counters, coverage map size {}",
        test_name, name_refs.size(), g_coverage_map.size());

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

    LOG_INFO(getLogger("CoverageCollection"),
        "Test '{}': {} counters resolved to {} unique (file, line) pairs (map_size={})",
        test_name, name_refs.size(), files.size(), g_coverage_map.size());

    if (files.empty())
        return;

    /// Build the INSERT query: flat schema, one row per region.
    /// Schema: coverage_log (time, test_name, file, line_start, line_end, min_depth, branch_flag)
    WriteBufferFromOwnString query_buf;
    writeString(
        "INSERT INTO system.coverage_log"
        " (time, test_name, file, line_start, line_end, min_depth, branch_flag)"
        " VALUES ", query_buf);
    for (size_t i = 0; i < files.size(); ++i)
    {
        if (i > 0)
            writeChar(',', query_buf);
        writeString("(now(),", query_buf);
        writeQuotedString(test_name, query_buf);
        writeChar(',', query_buf);
        writeQuotedString(files[i], query_buf);
        writeChar(',', query_buf);
        writeIntText(line_starts[i], query_buf);
        writeChar(',', query_buf);
        writeIntText(line_ends[i], query_buf);
        writeChar(',', query_buf);
        writeIntText(static_cast<uint32_t>(min_depths[i]), query_buf);
        writeChar(',', query_buf);
        writeIntText(static_cast<uint32_t>(branch_flags[i]), query_buf);
        writeChar(')', query_buf);
    }

    const std::string query = query_buf.str();

    try
    {
        auto query_context = Context::createCopy(context->getGlobalContext());
        query_context->makeQueryContext();
        query_context->setCurrentQueryId({});
        query_context->setSetting("max_query_size", Field{0ULL});
        query_context->setSetting("async_insert", Field{0ULL});
        auto block_io = executeQuery(query, query_context, QueryFlags{.internal = true}).second;
        /// For a VALUES INSERT with async_insert=0, executeQuery returns a "completed"
        /// pipeline (source=Values parser, sink=MergeTreeSink).  Calling onFinish()
        /// alone only resets the pipeline without running it — data would be lost.
        /// executeTrivialBlockIO executes the pipeline first, then finalises.
        executeTrivialBlockIO(block_io, query_context);
        LOG_INFO(getLogger("CoverageCollection"), "Inserted coverage for test '{}': {} regions", test_name, files.size());
    }
    catch (const Exception & e)
    {
        LOG_WARNING(getLogger("CoverageCollection"),
            "Failed to insert coverage for test '{}': code={} msg={}",
            test_name, e.code(), e.message());
    }
    catch (...)
    {
        LOG_WARNING(getLogger("CoverageCollection"),
            "Failed to insert coverage for test '{}': unknown exception",
            test_name);
    }

    /// Insert indirect-call observations into system.coverage_indirect_calls.
    /// Schema: (test_name String, caller_name_hash UInt64, caller_func_hash UInt64,
    ///          callee_offset UInt64, call_count UInt64)
    if (!indirect_calls.empty())
    {
        try
        {
            WriteBufferFromOwnString ic_buf;
            writeString(
                "INSERT INTO system.coverage_indirect_calls"
                " (test_name, caller_name_hash, caller_func_hash, callee_offset, call_count)"
                " VALUES ", ic_buf);
            bool first = true;
            for (const auto & ic : indirect_calls)
            {
                if (!first)
                    writeChar(',', ic_buf);
                first = false;
                writeChar('(', ic_buf);
                writeQuotedString(test_name, ic_buf);
                writeChar(',', ic_buf); writeIntText(ic.caller_name_hash, ic_buf);
                writeChar(',', ic_buf); writeIntText(ic.caller_func_hash, ic_buf);
                writeChar(',', ic_buf); writeIntText(ic.callee_offset, ic_buf);
                writeChar(',', ic_buf); writeIntText(ic.call_count, ic_buf);
                writeChar(')', ic_buf);
            }
            auto ic_context = Context::createCopy(context->getGlobalContext());
            ic_context->makeQueryContext();
            ic_context->setCurrentQueryId({});
            ic_context->setSetting("max_query_size", Field{0ULL});
            ic_context->setSetting("async_insert", Field{0ULL});
            auto ic_bio = executeQuery(ic_buf.str(), ic_context, QueryFlags{.internal = true}).second;
            executeTrivialBlockIO(ic_bio, ic_context);
        }
        catch (...) {} /// Ok: best-effort; indirect call data is supplementary, failure is non-fatal
    }
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

}

#endif
