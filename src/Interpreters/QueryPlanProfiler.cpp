#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/SensitiveDataMasker.h>
#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryPlanProfiler.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/FormatSettings.h>
#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/StepStatsStorage.h>
#include <Processors/QueryPlan/QueryPlanToJSON.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/StepWallClockRegistry.h>
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

    /// The step types that can hold an expression referencing a set. `SourceStepWithFilter` covers
    /// the reads generically, which is where a pushed-down `PREWHERE` puts the condition.
    const auto collect_from_step = [&](IQueryPlanStep & step)
    {
        if (auto * expression = dynamic_cast<ExpressionStep *>(&step))
            collect_from_dag(expression->getExpression(), step);
        else if (auto * filter = dynamic_cast<FilterStep *>(&step))
            collect_from_dag(filter->getExpression(), step);

        if (auto * source = dynamic_cast<SourceStepWithFilter *>(&step))
        {
            if (const auto & dag = source->getFilterActionsDAG())
                collect_from_dag(*dag, step);

            /// Where filter pushdown puts the condition, and therefore where an `IN` over an
            /// indexed column ends up: `s_suppkey IN subquery1` is a PREWHERE by the time the plan
            /// is optimized, not a `Filter` step of its own.
            if (const auto & prewhere = source->getPrewhereInfo())
                collect_from_dag(prewhere->prewhere_actions, step);
        }

        /// Set only when PREWHERE is deferred after FINAL, in which case it is the filter that
        /// actually runs.
        if (auto * read_from_merge_tree = dynamic_cast<ReadFromMergeTree *>(&step))
        {
            if (const auto & prewhere = read_from_merge_tree->getDeferredPrewhereInfo())
                collect_from_dag(prewhere->prewhere_actions, step);
            if (const auto & row_level = read_from_merge_tree->getDeferredRowLevelFilter())
                collect_from_dag(row_level->actions, step);
        }
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
        for (auto * child_plan : node->step->getChildPlans())
            if (child_plan && child_plan->getRootNode())
                stack.push_back(child_plan->getRootNode());
    }
}

void maskSensitiveValues(JSONBuilder::IItem & item)
{
    auto masker = SensitiveDataMasker::getInstance();
    if (!masker)
        return;

    item.transformStringValues([&](String & value) { masker->wipeSensitiveData(value); });
}

String toJSONString(JSONBuilder::ItemPtr item)
{
    maskSensitiveValues(*item);

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
        QueryPlanFormat::buildPrettyNamesPerPlan(*running.query_plan)
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
    if (internal)
        return false;

    const auto & settings = context->getSettingsRef();

    if (!settings[Setting::log_query_plans])
        return false;

    const auto declined = [&](const char * reason)
    {
        declineCapture(context, reason);
        return false;
    };

    /// The plan is stored on the `system.query_log` row, so without that row there is nowhere to
    /// put it and capturing would be pure cost.
    if (!settings[Setting::log_queries])
        return declined("setting `log_queries` is false, so the query writes no row to store it on");

    if (context->getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        return false;

    if (!isSupportedQuery(ast))
        return declined("only `SELECT` queries have their plan captured");

    if (!settings[Setting::allow_experimental_analyzer])
        return declined("setting `allow_experimental_analyzer` is false and the old analyzer cannot capture plans");

    if (settings[Setting::make_distributed_plan])
        return declined("setting `make_distributed_plan` is true and distributed execution is not supported");

    return true;
}

void QueryPlanProfiler::instrumentPipeline(QueryPipeline & pipeline) const
{
    if (!running.query_plan || !running.query_plan->isInitialized())
        return;

    auto registry = std::make_unique<StepWallClockRegistry>();
    registry->populateFromPlan(*running.query_plan);
    pipeline.setStepWallClockRegistry(std::move(registry));
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

void SubPlanCapture::publish(const StepStatsStorage * stats) noexcept
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
        auto registry = std::make_unique<StepWallClockRegistry>();
        registry->populateFromPlan(*plan);
        pipeline.setStepWallClockRegistry(std::move(registry));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void SubPlanCapture::finish(const QueryPipeline & pipeline)
{
    if (!profiler)
        return;

    std::optional<StepStatsStorage> stats;

    {
        MemoryTrackerBlockerInThread block_memory_tracker;

        try
        {
            UInt64 execution_time_ns = 0;
            if (const auto * registry = pipeline.getStepClocks())
                execution_time_ns = registry->getExecutionTimeNs();
            stats.emplace(pipeline, *plan, execution_time_ns);
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
            std::move(profiler), sub_plan, QueryPlanFormat::buildPrettyNamesPerPlan(sub_plan), subquery_id, kind);
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

void QueryPlanProfiler::captureStatistics(const QueryPipeline & pipeline)
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

void QueryPlanProfiler::capture(const QueryPipeline * pipeline)
{
    if (captured || !canCapture())
        return;

    /// Runs on the query-finish path after the client has already received the result.
    /// An exception here would fail a query that had already succeeded,
    /// so diagnostics must not propagate.
    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        std::optional<StepStatsStorage> stats;
        if (pipeline)
        {
            UInt64 execution_time_ns = 0;
            if (const auto * registry = pipeline->getStepClocks())
                execution_time_ns = registry->getExecutionTimeNs();
            stats.emplace(*pipeline, *running.query_plan, execution_time_ns);
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
    /// Rendering before `finish` would keep the plan -- and the table locks it owns -- alive for
    /// the whole of logging, which is what `finish` exists to prevent.
    chassert(finished);

    if (!captured)
        return {};

    auto capture = std::move(*captured);
    captured.reset();

    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        return toJSONString(capturedPlanToJSON(capture));
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
            /// Empty rather than invalid: the column takes its default, an empty JSON object.
            return {};
        }
    }
}
}
