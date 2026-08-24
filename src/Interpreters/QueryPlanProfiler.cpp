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

String QueryPlanProfiler::render(AnalyzeStepsStats * stats) const
{
    if (!canRender())
        return {};

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
        stats);
    out.finalize();
    return result;
}

void QueryPlanProfiler::renderWithStats(const QueryPipeline & pipeline)
{
    if (!canRender())
        return;

    UInt64 execution_time_ns = 0;
    if (const auto * registry = pipeline.getStepClocks())
        execution_time_ns = clock_gettime_ns() - registry->getQueryStartNs();

    AnalyzeStepsStats stats(pipeline, execution_time_ns);
    rendered_plan = render(&stats);
}
}
