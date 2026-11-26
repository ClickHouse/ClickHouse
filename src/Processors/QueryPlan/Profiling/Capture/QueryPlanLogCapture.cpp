#include <Common/Exception.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/logger_useful.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/thread_local_rng.h>
#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/Profiling/Capture/QueryPlanLogCapture.h>
#include <Processors/QueryPlan/QueryPlanShapeHash.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/FormatSettings.h>
#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/Profiling/Analysis/AnalyzePlanStats.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>
#include <Processors/QueryPlan/Profiling/Render/FlatJSONPlanRenderer.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/PreparedSets.h>
#include <QueryPipeline/QueryPipeline.h>

#include <random>

namespace DB
{

namespace Setting
{
extern const SettingsBool allow_experimental_analyzer;
extern const SettingsBool log_queries;
extern const SettingsBool log_query_plans;
extern const SettingsFloat log_query_plans_probability;
extern const SettingsBool make_distributed_plan;
}

namespace
{

/// Checks if the query is one whose plan can be captured
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

void maskMetrics(MetricList & metrics, const SensitiveDataMasker & masker)
{
    for (auto & metric : metrics)
        if (auto * text = std::get_if<std::string>(&metric.value))
            masker.wipeSensitiveData(*text);
}

/// Applies `query_masking_rules` to the text the statistics hold, as `CapturedPlan::capture` does
/// for the rest of the plan. Some of it comes from the query: a join reports the columns of its
/// inputs, and a constant column is named by its value. Only values are masked -- the keys of the
/// document are fixed names, given by the renderer.
void maskStatistics(AnalyzedStepData & statistics, const SensitiveDataMasker & masker)
{
    for (auto & group : statistics.step_metric_groups)
        maskMetrics(group.metrics, masker);

    /// Stage names are not masked: `getStepGroupName` gives fixed labels, like `build` and `probe`.
    for (auto & stage : statistics.stage_reports)
    {
        maskMetrics(stage.inline_metrics, masker);
        maskMetrics(stage.processor_distribution, masker);
    }
}

}

bool QueryPlanLogCapture::canCapturePlan(const ContextPtr & context, const ASTPtr & ast, std::string_view query, bool internal)
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

    /// The `query` column hides secret arguments, like the key of `encrypt`, by their position in
    /// the AST. In the plan they are constants inside expressions, possibly folded into derived
    /// values, and there is no reliable way to find them there, so no plan is stored at all.
    if (ast->hasSecretParts())
        return declined("the query has secret arguments, which the plan would show unmasked");

    /// Each text of the plan is masked when it is captured, but that only catches what still
    /// matches a rule. The plan holds what the analyzer made of the query: a masked literal can
    /// reach it transformed by constant folding -- `lower`, `substring`, `replace`, `hex` -- into a
    /// value no rule matches anymore. So a query in which a rule matched stores no plan. The values
    /// of query parameters are checked as they are, too: in the query they are quoted and escaped,
    /// which can hide a match the raw value would give. All parameters of the session are checked,
    /// not only those the query uses: once substituted, nothing records which ones those were.
    if (const auto masker = SensitiveDataMasker::getInstance())
    {
        if (masker->hasSensitiveData(query))
            return declined("`query_masking_rules` matched the query, and the plan could show values derived from the masked text");

        for (const auto & [name, value] : context->getQueryParameters())
            if (masker->hasSensitiveData(value))
                return declined("`query_masking_rules` matched a query parameter, and the plan could show its value");
    }

    /// Sampled last, so that only queries which could be captured take part. The comparison,
    /// unlike `std::bernoulli_distribution`, is defined for a probability outside of [0, 1].
    const auto probability = static_cast<double>(settings[Setting::log_query_plans_probability]);
    if (probability < 1.0 && std::uniform_real_distribution<double>(0.0, 1.0)(thread_local_rng) >= probability)
        return declined("the query was not selected by the sampling of setting `log_query_plans_probability`");

    return true;
}

void QueryPlanLogCapture::declineCapture(const ContextPtr & context, const char * reason)
{
    if (!context->getSettingsRef()[Setting::log_query_plans])
        return;

    LOG_TRACE(
        getLogger("QueryPlanLogCapture"),
        "Not storing the query plan in 'system.query_log' even though setting `log_query_plans`"
        " is true, because {}.",
        reason);
}

void QueryPlanLogCapture::capturePlanStructure(
    const PlanWalk & walk, const PrettyNamesPerPlan & pretty_names, const QueryPlanShapeHashes & shape_hashes, size_t max_description_length)
{
    /// One plan per query, and nothing is asked of this before it.
    chassert(!captured);

    /// Without statistics: the query has not run yet. They are filled in by `captureStatistics`.
    captured = CapturedPlan::capture(walk, planExplainOptions(), max_description_length, /*steps_to_stats=*/ nullptr, &pretty_names);

    captured->shape_hash = shape_hashes.root;
    for (auto & node : captured->nodes)
    {
        if (auto it = shape_hashes.steps.find(node.id); it != shape_hashes.steps.end())
            node.shape_hash = it->second;
    }
}

void QueryPlanLogCapture::captureStatistics(QueryPipeline & pipeline)
{
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

        const auto masker = SensitiveDataMasker::getInstance();
        for (auto & node : captured->nodes)
        {
            node.statistics = stats.analyzeStep(node.id);
            if (masker && node.statistics)
                maskStatistics(*node.statistics, *masker);
        }

        captured->execution_time_ns = stats.getExecutionTimeNs();
        captured->max_threads = stats.getMaxThreads();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
