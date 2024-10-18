#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Core/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 aggregation_memory_efficient_merge_threads;
}

BuildQueryPipelineSettings BuildQueryPipelineSettings::fromContext(ContextPtr from)
{
    BuildQueryPipelineSettings settings;
    settings.actions_settings = ExpressionActionsSettings::fromSettings(from->getSettingsRef(), CompileExpressions::yes);
    settings.process_list_element = from->getProcessListElement();
    settings.progress_callback = from->getProgressCallback();

    settings.max_threads = from->getSettingsRef()[Setting::max_threads];
    settings.aggregation_memory_efficient_merge_threads = from->getSettingsRef()[Setting::aggregation_memory_efficient_merge_threads];

    return settings;
}

}
