#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryPlanProfiler.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool log_query_plans;
}

std::shared_ptr<QueryPlanProfiler> QueryPlanProfiler::createIfEnabled(const ContextPtr & context, bool internal)
{
    if (internal)
        return nullptr;

    if (!context->getSettingsRef()[Setting::log_query_plans])
        return nullptr;

    if (context->getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        return nullptr;

    return std::make_shared<QueryPlanProfiler>();
}

String QueryPlanProfiler::renderAsciiPlan(size_t max_description_length) const
{
    if (!query_plan || !query_plan->isInitialized())
        return {};

    String result;
    WriteBufferFromString out(result);
    query_plan->explainPlan(out, ExplainPlanOptions{}, /*offset=*/ 0, max_description_length);
    out.finalize();
    return result;
}
}
