#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/Profiling/Capture/QueryPlanProfiler.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/FormatSettings.h>
#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/Profiling/Analysis/AnalyzePlanStats.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>
#include <Processors/QueryPlan/Profiling/Render/JsonPlanRenderer.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/PreparedSets.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool allow_experimental_analyzer;
extern const SettingsBool log_queries;
extern const SettingsBool log_query_plans;
extern const SettingsBool make_distributed_plan;
}

namespace
{

/// Checks if the query is supported by the profiler
bool isSupportedQuery(const ASTPtr & ast)
{
    return ast && (ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>());
}

/// `compact` and `pretty` are necessary as they are the only settings which makes describe
/// methods not read the `ActionsDAG`, that `buildQueryPipeline` has moved out.
ExplainPlanOptions planExplainOptions()
{
    return ExplainPlanOptions
    {
        .actions = true,
        .indexes = true,
        .projections = true,
        .compact = true,
        .pretty = true,
    };
}

}

bool QueryPlanProfiler::canEnableProfiler(const ContextPtr & context, const ASTPtr & ast, bool internal)
{
    const auto & settings = context->getSettingsRef();

    if (!settings[Setting::log_query_plans])
        return false;

    const auto declined = [&](const char * reason)
    {
        declineCapture(context, reason);
        return false;
    };

    /// An internal query writes a `system.query_log` row of its own, marked by `is_internal`.
    if (internal)
        return declined("the query is run internally by the server, and only queries issued by a client are captured");

    /// The plan is stored on the `system.query_log` row, so without that row there is nowhere to
    /// put it and capturing would be pure cost.
    if (!settings[Setting::log_queries])
        return declined("setting `log_queries` is false, so the query writes no row to store it on");

    /// Each shard of a distributed query writes a row of its own, and the plan of the whole query
    /// is captured on the initiator.
    if (context->getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        return declined("the query is a secondary query of a distributed query, and only the initial query is captured");

    if (!isSupportedQuery(ast))
        return declined("only `SELECT` queries have their plan captured");

    if (!settings[Setting::allow_experimental_analyzer])
        return declined("setting `allow_experimental_analyzer` is false and the old analyzer cannot capture plans");

    if (settings[Setting::make_distributed_plan])
        return declined("setting `make_distributed_plan` is true and distributed execution is not supported");

    return true;
}

void QueryPlanProfiler::declineCapture(const ContextPtr & context, const char * reason)
{
    if (!context->getSettingsRef()[Setting::log_query_plans])
        return;

    LOG_TRACE(
        getLogger("QueryPlanProfiler"),
        "Not storing the query plan in 'system.query_log' even though setting `log_query_plans`"
        " is true, because {}.",
        reason);
}

void QueryPlanProfiler::capturePlanStructure(const PlanWalk & walk, const PrettyNamesPerPlan & pretty_names, size_t max_description_length)
{
    /// One plan per query, and nothing is asked of the profiler before this.
    chassert(!captured);

    /// Without statistics: the query has not run yet. They are filled in by `captureStatistics`.
    captured = CapturedPlan::capture(walk, planExplainOptions(), max_description_length, /*steps_to_stats=*/ nullptr, &pretty_names);
}

void QueryPlanProfiler::captureStatistics(QueryPipeline & pipeline)
{
    /// The pipeline has owned it since it was built, which is why it is read back from there
    /// rather than kept here: nothing of the query outlives the pipeline.
    const StepProfilerPtr profiler = pipeline.getStepProfiler();

    if (!captured || !profiler)
        return;

    /// Runs on the query-finish path after the client has already received the result. An exception
    /// here would fail a query that had already succeeded, so diagnostics must not propagate.
    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        /// Both come from the profiler: the executor stamped the end when it finished, so the
        /// duration does not include whatever the query-finish path does afterwards.
        const AnalyzeStepsStats stats(
            pipeline,
            *profiler,
            profiler->getExecutionStartNs(),
            profiler->getExecutionTimeNs());

        for (auto & node : captured->nodes)
            node.statistics = stats.analyzeStep(node.id);

        captured->execution_time_ns = stats.getExecutionTimeNs();
        captured->max_threads = stats.getMaxThreads();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
