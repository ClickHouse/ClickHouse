#include <Common/Exception.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/Stopwatch.h>
#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryPlanProfiler.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/AnalyzePlanStats.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/StepWallClockRegistry.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool log_query_plans;
}

void QueryPlanProfiler::buildPrettyNames()
{
    if (query_plan.has_value())
    {
        pretty_names.emplace(
            QueryPlanFormat::buildPrettyNamesPerPlan(*query_plan)
        );
    }
}

bool QueryPlanProfiler::canEnableProfiler(const ContextPtr & context, bool internal)
{
    if (internal)
        return false;

    if (!context->getSettingsRef()[Setting::log_query_plans])
        return false;

    if (context->getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        return false;

    return true;
}

void QueryPlanProfiler::instrumentPipeline(QueryPipeline & pipeline) const
{
    if (!query_plan || !query_plan->isInitialized())
        return;

    auto registry = std::make_unique<StepWallClockRegistry>();
    registry->populateFromPlan(*query_plan);
    pipeline.setStepWallClockRegistry(std::move(registry));
}

void QueryPlanProfiler::render(const QueryPipeline * pipeline)
{
    if (!canRender())
    {
        rendered_plan.emplace();
        return;
    }

    /// Rendering runs on the query-finish path, which BlockIO::onFinish calls without a guard,
    /// after the client has already received the result. An exception here would fail a query that
    /// had already succeeded, so diagnostics must not propagate. The memory blocker keeps the plan
    /// text and the per-step statistics, both of which can be large, from being charged to the
    /// query and tripping max_memory_usage, matching what SystemLogQueue::add does for the row
    /// itself. Everything that allocates must stay inside the blocker and the try block.
    try
    {
        MemoryTrackerBlockerInThread block_memory_tracker;

        std::optional<AnalyzeStepsStats> stats;
        if (pipeline)
        {
            UInt64 execution_time_ns = 0;
            if (const auto * registry = pipeline->getStepClocks())
                execution_time_ns = clock_gettime_ns() - registry->getQueryStartNs();
            stats.emplace(*pipeline, execution_time_ns);
        }

        String result;
        WriteBufferFromString out(result);
        ExplainPlanOptions explain_options {
            .actions = true,
            .indexes = true,
            .compact = true,
            .pretty = true};
        query_plan->explainPlan(
            out,
            explain_options,
            /*offset=*/ 0,
            max_description_length,
            &pretty_names.value(),
            /*parent_tree_prefix=*/ "",
            /*is_last_child_plan=*/ true,
            stats ? &*stats : nullptr);
        out.finalize();
        rendered_plan = std::move(result);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        rendered_plan = "FAILED TO RENDER PLAN: " + getCurrentExceptionMessage(/*with_stacktrace=*/ false);
    }
}
}
