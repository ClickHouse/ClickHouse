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
        .compact = true,
        .pretty = true,
    };
}

/// Records, on every step that reads a set built by a subquery, the id of that subquery.
///
/// Must run while the `ActionsDAG`s are still in the plan.
void recordConsumedSubqueries(QueryPlan & plan)
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

    /// Every step is asked for the `ActionsDAG`s it owns, so a step type that can carry a set is
    /// never missed here: a `HAVING ... IN (SELECT ...)` survives as a `TotalsHavingStep`, and a
    /// pushed-down `PREWHERE` as part of the read.
    const auto collect_from_step = [&](IQueryPlanStep & step)
    {
        step.forEachActionsDAG([&](const ActionsDAG & dag) { collect_from_dag(dag, step); });
    };

    std::vector<QueryPlan::Node *> stack;
    if (plan.isInitialized())
        stack.push_back(plan.getRootNode());

    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();
        if (!node || !node->step)
            continue;

        collect_from_step(*node->step);

        for (auto * child : node->children)
            stack.push_back(child);

        /// A child plan is its own tree but the same query, and its steps can consume the same sets.
        /// Only the ones already built: asking a step to build them would be the capture changing
        /// what the query does.
        for (auto * child_plan : node->step->getBuiltChildPlans())
            if (child_plan && child_plan->getRootNode())
                stack.push_back(child_plan->getRootNode());
    }
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

QueryPlan & QueryPlanProfiler::captureQueryPlan(QueryPlan plan_)
{
    /// One plan per query, given before anything else is asked of the profiler.
    chassert(!running.query_plan);
    chassert(!finished);

    running.query_plan.emplace(std::move(plan_));

    /// Both of these read the ActionsDAGs, which building the pipeline moves out of the steps.
    recordConsumedSubqueries(*running.query_plan);
    running.pretty_names.emplace(
        QueryPlanFormat::buildPrettyNamesPerPlan(*running.query_plan, /*only_built_child_plans=*/ true)
    );
    return *running.query_plan;
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

void QueryPlanProfiler::instrumentPipeline(QueryPipeline & pipeline)
{
    if (!running.query_plan || !running.query_plan->isInitialized())
        return;

    /// Work intervals are only for `EXPLAIN ANALYZE`; the plan column does not render them yet.
    /// The clocks are attached from the plans the steps already hold, so instrumenting a pipeline
    /// does not make a step build anything.
    running.step_profiler = std::make_shared<StepProfiler>(
        *running.query_plan, /*collect_work_intervals_=*/ false, /*only_built_child_plans=*/ true);
    pipeline.setStepProfiler(running.step_profiler);
}

SubPlanCapture::SubPlanCapture(
    QueryPlanProfilerPtr profiler_,
    const QueryPlan & plan_,
    PrettyNamesPerPlan pretty_names_,
    size_t subquery_id_,
    SubPlanKind kind_)
    : profiler(std::move(profiler_))
    , plan(&plan_)
    , pretty_names(std::move(pretty_names_))
    , subquery_id(subquery_id_)
    , kind(kind_)
{
}

SubPlanCapture::SubPlanCapture(SubPlanCapture && other) noexcept
    : profiler(std::move(other.profiler))
    , plan(other.plan)
    , pretty_names(std::move(other.pretty_names))
    , subquery_id(other.subquery_id)
    , kind(other.kind)
{
    /// Leaves `other` inert, so only one of the two ever publishes.
    other.profiler.reset();
}

SubPlanCapture & SubPlanCapture::operator=(SubPlanCapture && other) noexcept
{
    if (this == &other)
        return *this;

    /// Whatever this capture was holding is finished with, and publishing the structure is better
    /// than discarding it. A no-op in the usual case, where the target is still inert.
    publish(nullptr);

    profiler = std::move(other.profiler);
    plan = other.plan;
    pretty_names = std::move(other.pretty_names);
    subquery_id = other.subquery_id;
    kind = other.kind;
    other.profiler.reset();

    return *this;
}

SubPlanCapture::~SubPlanCapture()
{
    /// Reached when `finish` never ran -- an exception while the sub-pipeline was executing, or a
    /// caller that stopped early.
    publish(nullptr);
}

void SubPlanCapture::publish(const AnalyzeStepsStats * stats) noexcept
{
    if (!profiler)
        return;

    /// Avoids publishing the plan a second time afterwards
    auto owner = std::move(profiler);

    /// Allocations belong to the profiler and no exception may escape.
    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        auto serialized = captureSubPlanData(
            *plan,
            planExplainOptions(),
            owner->max_description_length,
            subquery_id,
            kind,
            stats,
            &pretty_names);

        /// `captureSubPlan` already refused a plan without a root, so a rooted plan serializing to
        /// nothing means the walk and the plan disagree. The return keeps a release build from
        /// storing an entry whose `Root` names a node the document does not contain.
        chassert(!serialized.nodes.empty());
        if (serialized.nodes.empty())
            return;

        owner->addSubPlan(std::move(serialized));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void SubPlanCapture::instrument(QueryPipeline & pipeline)
{
    if (!profiler)
        return;

    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        step_profiler = std::make_shared<StepProfiler>(
            *plan, /*collect_work_intervals_=*/ false, /*only_built_child_plans=*/ true);
        pipeline.setStepProfiler(step_profiler);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void SubPlanCapture::finish(QueryPipeline & pipeline)
{
    if (!profiler)
        return;

    std::optional<AnalyzeStepsStats> stats;

    {
        MemoryTrackerBlockerInThread block_memory_tracker;

        try
        {
            if (step_profiler)
                stats.emplace(pipeline, *plan, *step_profiler,
                    step_profiler->getExecutionStartNs(), step_profiler->getExecutionTimeNs());
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    publish(stats ? &*stats : nullptr);
}

SubPlanCapture QueryPlanProfiler::captureSubPlan(
    const ContextPtr & context, QueryPlan & sub_plan, size_t subquery_id, SubPlanKind kind)
{
    auto profiler = context->getPlanProfiler();
    if (!profiler)
        return {};

    if (!sub_plan.isInitialized() || !sub_plan.getRootNode())
        return {};

    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        recordConsumedSubqueries(sub_plan);

        return SubPlanCapture(
            std::move(profiler),
            sub_plan,
            QueryPlanFormat::buildPrettyNamesPerPlan(sub_plan, /*only_built_child_plans=*/ true),
            subquery_id,
            kind);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return {};
    }
}

void QueryPlanProfiler::addSubPlan(CapturedSubPlan sub_plan)
{
    std::lock_guard lock(running.sub_plans_mutex);
    running.sub_plans.push_back(std::move(sub_plan));
}

void QueryPlanProfiler::captureStatistics(QueryPipeline & pipeline)
{
    /// Otherwise there is nothing for the statistics to be about, and the pipeline that produced
    /// them was built from a plan this profiler never saw.
    chassert(running.query_plan);
    chassert(!finished);

    capture(&pipeline);
}

void QueryPlanProfiler::finish()
{
    if (finished)
        return;

    /// Nothing captured means the query never reached `captureStatistics`; take the plan alone.
    if (!captured)
        capture(nullptr);

    running.release();
    finished = true;
}

void QueryPlanProfiler::capture(QueryPipeline * pipeline)
{
    if (captured || !canCapture())
        return;

    /// Runs on the query-finish path after the client has already received the result.
    /// An exception here would fail a query that had already succeeded,
    /// so diagnostics must not propagate.
    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        std::optional<AnalyzeStepsStats> stats;
        if (pipeline && running.step_profiler)
        {
            /// Both come from the profiler: the executor stamped the end when it finished, so the
            /// duration does not include whatever the query-finish path does afterwards.
            stats.emplace(*pipeline, *running.query_plan, *running.step_profiler,
                running.step_profiler->getExecutionStartNs(), running.step_profiler->getExecutionTimeNs());
        }

        auto result = capturePlan(
            *running.query_plan,
            planExplainOptions(),
            max_description_length,
            stats ? &*stats : nullptr,
            running.pretty_names ? &*running.pretty_names : nullptr);

        std::lock_guard lock(running.sub_plans_mutex);
        result.sub_plans = std::move(running.sub_plans);
        captured = std::move(result);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

String QueryPlanProfiler::render()
{
    chassert(finished);

    if (!captured)
        return {};

    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        return toJSONString(capturedPlanToJSON(*captured));
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
