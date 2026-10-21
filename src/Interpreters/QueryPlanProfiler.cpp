#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryPlanProfiler.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/FormatSettings.h>
#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/Profiling/Analysis/AnalyzePlanStats.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>
#include <Processors/QueryPlan/Profiling/Render/JsonPlanRenderer.h>
#include <Processors/QueryPlan/QueryPlanToJSON.h>
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

String toJSONString(JSONBuilder::ItemPtr item)
{
    FormatSettings format_settings;
    format_settings.json.quote_64bit_integers = false;

    String result;
    WriteBufferFromString out(result);
    JSONBuilder::FormatSettings json_format_settings{.settings = format_settings};
    JSONBuilder::FormatContext format_context{.out = out};
    item->format(json_format_settings, format_context);
    out.finalize();

    return result;
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

PrettyNamesPerPlan QueryPlanProfiler::capturePrettyNames(const QueryPlan & plan)
{
    /// Only the child plans the steps already hold: asking a step to build one would make taking
    /// the names change what the query does.
    return QueryPlanFormat::buildPrettyNamesPerPlan(plan, /*only_built_child_plans=*/ true);
}

void QueryPlanProfiler::capturePlanStructure(const QueryPlan & plan, const PrettyNamesPerPlan & pretty_names)
{
    /// One plan per query, and nothing is asked of the profiler before this.
    chassert(!captured);
    chassert(!finished);

    /// Runs while the query is still being set up, so an exception here would fail a query that
    /// would otherwise have run. Capturing is a diagnostic; it does not get to do that.
    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        /// Without statistics: the query has not run yet. They are filled in by `captureStatistics`.
        captured = capturePlan(plan, planExplainOptions(), max_description_length, /*steps_to_stats=*/ nullptr, &pretty_names);

        plan_snapshot = PlanAnalysisSnapshot::capture(plan);

        /// Takes a clock per step of the plan, so it has to be built while the plan is here.
        step_profiler = std::make_shared<StepProfiler>(plan, /*collect_work_intervals_=*/ false, /*only_built_child_plans=*/ true);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        captured.reset();
        step_profiler.reset();
    }
}

void QueryPlanProfiler::captureStatistics(QueryPipeline & pipeline)
{
    chassert(!finished);

    if (!captured || !step_profiler)
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
            plan_snapshot,
            *step_profiler,
            step_profiler->getExecutionStartNs(),
            step_profiler->getExecutionTimeNs());

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

void QueryPlanProfiler::instrumentPipeline(QueryPipeline & pipeline)
{
    /// Built with the plan, back when there was one.
    if (step_profiler)
        pipeline.setStepProfiler(step_profiler);
}

void QueryPlanProfiler::finish()
{
    if (finished)
        return;

    /// A query that threw keeps the plan captured when its pipeline was built, without statistics.
    /// One that threw before the pipeline was built captured nothing, and has no plan to show:
    /// there is no executed plan for a query that never got one.
    step_profiler.reset();
    plan_snapshot = {};
    finished = true;
}

String QueryPlanProfiler::render()
{
    chassert(finished);

    if (!captured)
        return {};

    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        String result;
        WriteBufferFromString out(result);
        JsonPlanRenderer renderer;
        renderer.render(*captured, out);
        out.finalize();
        return result;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        try
        {
            auto error_map = std::make_unique<JSONBuilder::JSONMap>();
            error_map->add("Error", getCurrentExceptionMessage(/*with_stacktrace=*/ false));
            return toJSONString(std::move(error_map));
        }
        catch (...)
        {
            /// Ok to swallow: rendering the error itself failed, and this runs on the logging path
            /// of a query that already returned its result. Empty rather than invalid, so the
            /// column takes its default of an empty JSON object.
            return {};
        }
    }
}
}
