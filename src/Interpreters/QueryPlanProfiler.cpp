#include <Common/Exception.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/Stopwatch.h>
#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryPlanProfiler.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/FormatSettings.h>
#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/StepStatsStorage.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/StepWallClockRegistry.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool log_query_plans;
}

namespace
{

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

void QueryPlanProfiler::setQueryPlan(QueryPlan plan_)
{
    query_plan.emplace(std::move(plan_));
    pretty_names.emplace(
        QueryPlanFormat::buildPrettyNamesPerPlan(*query_plan)
    );
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
        plan_json.emplace();
        return;
    }

    /// Rendering runs on the query-finish path, which BlockIO::onFinish calls without a guard,
    /// after the client has already received the result. An exception here would fail a query that
    /// had already succeeded, so diagnostics must not propagate.
    MemoryTrackerBlockerInThread block_memory_tracker;

    try
    {
        std::optional<StepStatsStorage> stats;
        if (pipeline)
        {
            UInt64 execution_time_ns = 0;
            if (const auto * registry = pipeline->getStepClocks())
                execution_time_ns = clock_gettime_ns() - registry->getQueryStartNs();
            stats.emplace(*pipeline, execution_time_ns);
        }

        ExplainPlanOptions explain_options
        {
            .actions = true,
            .indexes = true,
            .compact = true,
            .pretty = true,
        };

        plan_json = toJSONString(query_plan->explainPlan(
            explain_options,
            max_description_length,
            stats ? &*stats : nullptr,
            pretty_names ? &*pretty_names : nullptr));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        try
        {
            /// The result is written into a JSON column, so a failure has to be reported as JSON as
            /// well: a bare message would fail to parse in QueryLogElement::appendToBlock and take
            /// the whole log flush with it. Going through JSONBuilder also escapes whatever the
            /// exception message happens to contain.
            auto error_map = std::make_unique<JSONBuilder::JSONMap>();
            error_map->add("Error", getCurrentExceptionMessage(/*with_stacktrace=*/ false));
            plan_json = toJSONString(std::move(error_map));
        }
        catch (...)
        {
            /// Empty rather than invalid: the column takes its default, an empty JSON object.
            plan_json.emplace();
        }
    }
}
}
