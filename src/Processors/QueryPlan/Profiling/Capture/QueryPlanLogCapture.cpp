#include <Common/Exception.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/logger_useful.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/Profiling/Capture/QueryPlanLogCapture.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/FormatSettings.h>
#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Profiling/Analysis/AnalyzePlanStats.h>
#include <Processors/QueryPlan/Profiling/Capture/PlanWalk.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Profiling/Render/FlatJSONPlanRenderer.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/ActionsDAG.h>
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
extern const SettingsUInt64 query_plan_max_step_description_length;
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

void QueryPlanLogCapture::recordConsumedSubqueries(QueryPlan & plan)
{
    const auto collect_from_dag = [](const ActionsDAG & dag, IQueryPlanStep & step)
    {
        /// Scalar subqueries, whose folded value may no longer be identifiable node by node.
        for (size_t id : dag.getScalarSubqueryIds())
            step.addConsumedSubqueryId(id);

        for (const auto & node : dag.getNodes())
        {
            if (node.type != ActionsDAG::ActionType::COLUMN || !node.column)
                continue;

            /// A scalar subquery leaves only its value behind, so the planner wrote the ids onto
            /// the constant as it built the actions.
            for (size_t id : node.scalar_subquery_ids)
                step.addConsumedSubqueryId(id);

            const auto * column_set = checkAndGetColumn<const ColumnSet>(&node.column->getDataColumn());
            if (!column_set)
                continue;

            const auto future_set = column_set->getData();
            if (const auto * from_subquery = dynamic_cast<const FutureSetFromSubquery *>(future_set.get()))
                step.addConsumedSubqueryId(from_subquery->getSubqueryId());
        }
    };

    VectorWithMemoryTracking<QueryPlan::Node *> stack;
    if (plan.isInitialized())
        stack.push_back(plan.getRootNode());

    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();
        if (!node || !node->step)
            continue;

        /// Every step is asked for the `ActionsDAG`s it owns, so a step type that can carry a set
        /// is never missed here: a `HAVING ... IN (SELECT ...)` survives as a `TotalsHavingStep`,
        /// and a pushed-down `PREWHERE` as part of the read.
        auto & step = *node->step;
        step.forEachActionsDAG([&](const ActionsDAG & dag) { collect_from_dag(dag, step); });

        for (auto * child : node->children)
            stack.push_back(child);

        /// A child plan is its own tree but the same query, and its steps can consume the same sets.
        /// Only the ones already built: asking a step to build them would be the capture changing
        /// what the query does.
        for (auto * child_plan : step.getBuiltChildPlans())
            if (child_plan && child_plan->getRootNode())
                stack.push_back(child_plan->getRootNode());
    }
}

void QueryPlanLogCapture::capturePlanStructure(const PlanWalk & walk, const PrettyNamesPerPlan & pretty_names, size_t max_description_length)
{
    /// Without statistics: the query has not run yet. They are filled in by `captureStatistics`.
    auto plan = CapturedPlan::capture(walk, planExplainOptions(), max_description_length, /*steps_to_stats=*/ nullptr, &pretty_names);

    std::lock_guard lock(mutex);

    /// One plan per query, and nothing is asked of this before it.
    chassert(!captured);

    plan.sub_plans = std::move(pending_sub_plans);
    pending_sub_plans.clear();
    captured = std::move(plan);
}

void QueryPlanLogCapture::dropCapturedPlan()
{
    std::lock_guard lock(mutex);
    captured.reset();
}

void QueryPlanLogCapture::addSubPlan(CapturedSubPlan sub_plan)
{
    std::lock_guard lock(mutex);
    if (captured)
        captured->sub_plans.push_back(std::move(sub_plan));
    else
        pending_sub_plans.push_back(std::move(sub_plan));
}

SubPlanCapture QueryPlanLogCapture::captureSubPlan(
    const ContextPtr & context,
    QueryPlan & sub_plan,
    const QueryPlanOptimizationSettings & optimization_settings,
    size_t subquery_id,
    SubPlanKind kind)
{
    auto owner = context->getPlanLogCapture();
    if (!owner)
        return {};

    /// Still set only if `applyDistributedPlanFallbackToLocal` accepted the plan. Possible for a
    /// scalar subquery, whose own `SETTINGS` can ask for a distributed plan that the query did not.
    if (optimization_settings.make_distributed_plan)
        return {};

    if (!sub_plan.isInitialized() || !sub_plan.getRootNode())
        return {};

    /// Profiling is a diagnostic and does not get to fail a query that would otherwise run.
    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        recordConsumedSubqueries(sub_plan);

        return SubPlanCapture(
            std::move(owner),
            sub_plan,
            QueryPlanFormat::buildPrettyNamesPerPlan(sub_plan, /*only_built_child_plans=*/ true),
            context->getSettingsRef()[Setting::query_plan_max_step_description_length],
            subquery_id,
            kind);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return {};
    }
}

SubPlanCapture::SubPlanCapture(
    QueryPlanLogCapturePtr owner_,
    const QueryPlan & plan_,
    PrettyNamesPerPlan pretty_names_,
    size_t max_description_length_,
    size_t subquery_id_,
    SubPlanKind kind_)
    : owner(std::move(owner_))
    , plan(&plan_)
    , pretty_names(std::move(pretty_names_))
    , max_description_length(max_description_length_)
    , subquery_id(subquery_id_)
    , kind(kind_)
{
}

SubPlanCapture::SubPlanCapture(SubPlanCapture && other) noexcept
    : owner(std::move(other.owner))
    , plan(other.plan)
    , pretty_names(std::move(other.pretty_names))
    , max_description_length(other.max_description_length)
    , subquery_id(other.subquery_id)
    , kind(other.kind)
    , captured(std::move(other.captured))
{
    /// Leaves `other` inert, so only one of the two ever publishes.
    other.owner.reset();
    other.captured.reset();
}

SubPlanCapture & SubPlanCapture::operator=(SubPlanCapture && other) noexcept
{
    if (this == &other)
        return *this;

    /// Whatever this capture was holding is finished with, and publishing it is better than
    /// discarding it. A no-op in the usual case, where the target is still inert.
    publish();

    owner = std::move(other.owner);
    plan = other.plan;
    pretty_names = std::move(other.pretty_names);
    max_description_length = other.max_description_length;
    subquery_id = other.subquery_id;
    kind = other.kind;
    captured = std::move(other.captured);
    other.owner.reset();
    other.captured.reset();

    return *this;
}

SubPlanCapture::~SubPlanCapture()
{
    /// Reached without `finish` when the sub-pipeline threw or the caller stopped early. The shape
    /// is published without statistics.
    publish();
}

void SubPlanCapture::publish() noexcept
{
    if (!owner || !captured)
        return;

    /// Avoids publishing the plan a second time afterwards.
    auto target = std::move(owner);
    owner.reset();

    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        target->addSubPlan(std::move(*captured));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    captured.reset();
}

void SubPlanCapture::instrument(QueryPipeline & pipeline)
{
    if (!owner)
        return;

    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        /// Read now, as the query's own plan is: the index analysis and the child plans built along
        /// the way exist only once the pipeline has been built.
        const PlanWalk walk = PlanWalk::of(*plan, /*only_built_child_plans=*/ true);
        auto sub_plan = CapturedSubPlan::capture(walk, planExplainOptions(), max_description_length, &pretty_names, subquery_id, kind);

        /// `captureSubPlan` refused a plan without a root, so a rooted plan read as nothing means the
        /// walk and the plan disagree. Not storing it keeps a release build from writing an entry
        /// whose `Root` names a node the document does not contain.
        chassert(!sub_plan.nodes.empty());
        if (sub_plan.nodes.empty())
        {
            owner.reset();
            return;
        }

        pipeline.setStepProfiler(std::make_shared<StepProfiler>(walk, /*collect_work_intervals_=*/ false));
        captured = std::move(sub_plan);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        owner.reset();
        captured.reset();
    }
}

void SubPlanCapture::finish(QueryPipeline & pipeline)
{
    if (!owner || !captured)
        return;

    const StepProfilerPtr profiler = pipeline.getStepProfiler();
    if (profiler)
    {
        MemoryTrackerBlockerInThread block_memory_tracker;

        try
        {
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

    publish();
}

void QueryPlanLogCapture::captureStatistics(QueryPipeline & pipeline)
{
    const StepProfilerPtr profiler = pipeline.getStepProfiler();

    std::lock_guard lock(mutex);
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
