#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Core/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_NOT_ALLOWED;
}

BuildQueryPipelineSettings BuildQueryPipelineSettings::fromContext(ContextPtr from)
{
    BuildQueryPipelineSettings settings;
    settings.actions_settings = ExpressionActionsSettings::fromSettings(from->getSettingsRef(), CompileExpressions::yes);
    settings.process_list_element = from->getProcessListElement();
    settings.progress_callback = from->getProgressCallback();
    return settings;
}

}
