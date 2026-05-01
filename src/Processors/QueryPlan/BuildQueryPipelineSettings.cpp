#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool query_plan_merge_filters;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 aggregation_memory_efficient_merge_threads;
    extern const SettingsUInt64 min_outstreams_per_resize_after_split;
    extern const SettingsUInt64 max_streams_for_union_step;
    extern const SettingsFloat max_streams_for_union_step_to_max_threads_ratio;
}

BuildQueryPipelineSettings::BuildQueryPipelineSettings(ContextPtr from)
{
    const auto & settings = from->getSettingsRef();

    actions_settings = ExpressionActionsSettings(settings, CompileExpressions::yes);
    process_list_element = from->getProcessListElement();
    progress_callback = from->getProgressCallback();

    max_threads = from->getSettingsRef()[Setting::max_threads];
    aggregation_memory_efficient_merge_threads = from->getSettingsRef()[Setting::aggregation_memory_efficient_merge_threads];
    min_outstreams_per_resize_after_split = from->getSettingsRef()[Setting::min_outstreams_per_resize_after_split];
    max_streams_for_union_step = from->getSettingsRef()[Setting::max_streams_for_union_step];
    max_streams_for_union_step_to_max_threads_ratio = from->getSettingsRef()[Setting::max_streams_for_union_step_to_max_threads_ratio];

    /// Setting query_plan_merge_filters is enabled by default.
    /// But it can brake short-circuit without splitting filter step into smaller steps.
    /// So, enable and disable this optimizations together.
    enable_multiple_filters_transforms_for_and_chain = settings[Setting::query_plan_merge_filters];

    block_marshalling_callback = from->getBlockMarshallingCallback();
}

}
