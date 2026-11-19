#if defined(__ELF__) && !defined(OS_FREEBSD) && WITH_COVERAGE_DEPTH

#include <Common/CoverageCollection.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <QueryPipeline/BlockIO.h>
#include <base/coverage.h>

#include <string>
#include <vector>


namespace DB
{

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

    LOG_INFO(getLogger("CoverageCollection"), "Flushing test '{}': {} covered counters", test_name, name_refs.size());

    ResolvedCoverage resolved = resolveCoverage(name_refs);
    auto & files = resolved.files;
    auto & line_starts = resolved.line_starts;
    auto & line_ends = resolved.line_ends;
    auto & min_depths = resolved.min_depths;
    auto & branch_flags = resolved.branch_flags;

    LOG_INFO(getLogger("CoverageCollection"),
        "Test '{}': {} counters resolved to {} unique (file, line) pairs",
        test_name, name_refs.size(), files.size());

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
        /// Keep the flushes out of `system.query_log`: tests that count its rows would see them.
        query_context->setSetting("log_queries", Field{0ULL});
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
            ic_context->setSetting("log_queries", Field{0ULL});
            auto ic_bio = executeQuery(ic_buf.str(), ic_context, QueryFlags{.internal = true}).second;
            executeTrivialBlockIO(ic_bio, ic_context);
        }
        catch (...) {} /// Ok: best-effort; indirect call data is supplementary, failure is non-fatal
    }
}

}

#endif
