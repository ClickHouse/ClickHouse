#include <Interpreters/InterpreterSelectQueryFromPlan.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool use_concurrency_control;
}

InterpreterSelectQueryFromPlan::InterpreterSelectQueryFromPlan(
    QueryPlan && query_plan_,
    const ContextPtr & context_,
    const SelectQueryOptions & select_query_options_)
    : query_plan(std::move(query_plan_))
    , context(Context::createCopy(context_))
    , select_query_options(select_query_options_)
{
}

BlockIO InterpreterSelectQueryFromPlan::execute()
{
    QueryPlanOptimizationSettings optimization_settings(context);
    BuildQueryPipelineSettings build_pipeline_settings(context);

    query_plan.setConcurrencyControl(context->getSettingsRef()[Setting::use_concurrency_control]);

    auto pipeline_builder = std::move(*query_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings));

    BlockIO result;
    result.pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));

    if (!select_query_options.ignore_quota && select_query_options.to_stage == QueryProcessingStage::Complete)
        result.pipeline.setQuota(context->getQuota());

    return result;
}

}
