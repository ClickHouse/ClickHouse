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
#include <Processors/QueryPlan/StepStatsStorage.h>
#include <Processors/QueryPlan/QueryPlanToJSON.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/StepWallClockRegistry.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool allow_experimental_analyzer;
extern const SettingsBool log_queries;
extern const SettingsBool log_query_plans;
}

namespace
{

/// The statements whose plan can be captured: those InterpreterFactory routes to
/// InterpreterSelectQueryAnalyzer, currently the only interpreter that supports plan profiling.
/// Widen this as other interpreters gain support -- it is the single place that decides which
/// queries pay for the capture.
bool isSupportedQuery(const ASTPtr & ast)
{
    return ast && (ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>());
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

QueryPlan & QueryPlanProfiler::setQueryPlan(QueryPlan plan_)
{
    query_plan.emplace(std::move(plan_));
    pretty_names.emplace(
        QueryPlanFormat::buildPrettyNamesPerPlan(*query_plan)
    );
    return *query_plan;
}

bool QueryPlanProfiler::canEnableProfiler(const ContextPtr & context, const ASTPtr & ast, bool internal)
{
    if (internal)
        return false;

    const auto & settings = context->getSettingsRef();

    if (!settings[Setting::log_query_plans])
        return false;

    /// From here on the query asked for its plan, so every remaining way of saying no leaves the
    /// `query_plan` column empty with nothing on the surface to explain it. Say why. The reasons
    /// below are the ones a user can act on; the two above are not, and the secondary-query case is
    /// by design -- the initial query logs the plan for all of them -- so none of those speak.
    const auto declined = [](const char * reason)
    {
        LOG_TRACE(
            getLogger("QueryPlanProfiler"),
            "Not storing the query plan in 'system.query_log' even though setting `log_query_plans`"
            " is true, because {}.",
            reason);
        return false;
    };

    /// The plan is stored on the `system.query_log` row, so without that row there is nowhere to
    /// put it and capturing would be pure cost.
    if (!settings[Setting::log_queries])
        return declined("setting `log_queries` is false, so the query writes no row to store it on");

    if (context->getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        return false;

    /// Asks the statement rather than the interpreter because the join analyze mode has to be
    /// decided before the interpreter exists: that is the last moment at which it still reaches
    /// the planner.
    if (!isSupportedQuery(ast))
        return declined("only `SELECT` queries have their plan captured");

    if (!settings[Setting::allow_experimental_analyzer])
        return declined("setting `allow_experimental_analyzer` is false and the old analyzer cannot capture plans");

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

const String & QueryPlanProfiler::render(const QueryPipeline * pipeline)
{
    /// Rendering twice would throw away the version that has the statistics, and the second call
    /// would have no plan left to read anyway.
    if (plan_json)
        return *plan_json;

    if (!canRender())
        return plan_json.emplace();

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
                execution_time_ns = registry->getExecutionTimeNs();
            stats.emplace(*pipeline, *query_plan, execution_time_ns);
        }

        ExplainPlanOptions explain_options
        {
            .actions = true,
            .indexes = true,
            .compact = true,
            .pretty = true,
        };

        plan_json = toJSONString(queryPlanToJSON(
            *query_plan,
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
        catch (...) /// Ok: reporting the failure has itself failed, and this runs on the
                    /// query-finish path of a query that already returned its result. The first
                    /// exception was logged above; leaving the plan empty costs the row its plan
                    /// and nothing else, whereas letting this one out would fail a query that
                    /// succeeded.
        {
            /// Empty rather than invalid: the column takes its default, an empty JSON object.
            plan_json.emplace();
        }
    }

    releasePlan();

    return *plan_json;
}
}
