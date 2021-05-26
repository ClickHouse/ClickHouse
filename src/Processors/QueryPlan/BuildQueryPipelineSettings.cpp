#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Core/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>

namespace DB
{

BuildQueryPipelineSettings BuildQueryPipelineSettings::fromSettings(const Settings & from)
{
    BuildQueryPipelineSettings settings;
    settings.actions_settings = ExpressionActionsSettings::fromSettings(from);
    return settings;
}

BuildQueryPipelineSettings BuildQueryPipelineSettings::fromContext(const Context & from)
{
    return fromSettings(from.getSettingsRef());
}

}
