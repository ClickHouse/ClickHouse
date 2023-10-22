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

    const auto & context_settings = from->getSettingsRef();
    settings.partial_result_limit = context_settings.max_rows_in_partial_result;
    settings.partial_result_duration_ms = context_settings.partial_result_update_duration_ms.totalMilliseconds();
    if (settings.partial_result_duration_ms && !context_settings.allow_experimental_partial_result)
        throw Exception(ErrorCodes::FUNCTION_NOT_ALLOWED,
            "Partial results are not allowed by default, it's an experimental feature. "
            "Setting 'allow_experimental_partial_result' must be enabled to use 'partial_result_update_duration_ms'");

    settings.actions_settings = ExpressionActionsSettings::fromSettings(context_settings, CompileExpressions::yes);
    settings.process_list_element = from->getProcessListElement();
    settings.progress_callback = from->getProgressCallback();
    return settings;
}

}
