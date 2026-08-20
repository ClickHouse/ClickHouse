#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryPlanProfiler.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool log_query_plans;
}

void QueryPlanProfiler::buildPrettyNames() {
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

String QueryPlanProfiler::renderAsciiPlan(size_t max_description_length) const
{
    if (!query_plan || !query_plan->isInitialized() || !pretty_names)
        return {};

    String result;
    WriteBufferFromString out(result);
    query_plan->explainPlan(out, ExplainPlanOptions{.pretty=true}, /*offset=*/ 0, max_description_length, &pretty_names.value());
    out.finalize();
    return result;
}
}
