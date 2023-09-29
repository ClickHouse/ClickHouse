#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Core/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>

namespace DB
{

BuildQueryPipelineSettings BuildQueryPipelineSettings::fromContext(ContextPtr from)
{
    BuildQueryPipelineSettings settings;

    const auto & context_settings = from->getSettingsRef();
    settings.partial_result_limit = context_settings.max_rows_in_partial_result;
    settings.partial_result_duration_ms = context_settings.partial_result_update_duration_ms.totalMilliseconds();

    settings.actions_settings = ExpressionActionsSettings::fromSettings(context_settings, CompileExpressions::yes);
    settings.process_list_element = from->getProcessListElement();
    settings.progress_callback = from->getProgressCallback();
    return settings;
}

}
