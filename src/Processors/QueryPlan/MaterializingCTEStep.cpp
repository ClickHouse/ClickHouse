#include <exception>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
//#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include "Common/CurrentThread.h"
#include <Common/JSONBuilder.h>
#include "Interpreters/Context_fwd.h"
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

MaterializingCTEStep::MaterializingCTEStep(
    const DataStream & input_stream_,
    StoragePtr external_table_,
    String cte_table_name_,
    SizeLimits network_transfer_limits_,
    ContextPtr context_)
    : ITransformingStep(input_stream_, Block{}, getTraits())
    , WithContext(context_)
    , cte_table_name(std::move(cte_table_name_))
    , external_table(std::move(external_table_))
    , network_transfer_limits(std::move(network_transfer_limits_))
{
}

void MaterializingCTEStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    // pipeline.addCreatingSetsTransform(getOutputStream().header, std::move(set_and_key), std::move(external_table), network_transfer_limits, context->getPreparedSetsCache());
    pipeline.addMaterializingCTEsTransform(getOutputStream().header, external_table, cte_table_name, network_transfer_limits);
}

void MaterializingCTEStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), Block{}, getDataStreamTraits());
}

}
