#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include "config.h"

#if USE_GPU

#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/GPUAggregatingStep.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// An aggregation that a rewrite made after the planner, such as the decorrelation of a subquery, is an `AggregatingStep`
/// where the planner would have made a `GPUAggregatingStep`: it is made one here, where the device can run it.
void replaceWithGPUAggregating(QueryPlan::Node & node, const QueryPlanOptimizationSettings & settings)
{
    const auto * aggregating = typeid_cast<const AggregatingStep *>(node.step.get());
    if (!aggregating || node.children.size() != 1 || !settings.gpu_aggregation)
        return;

    /// What the planner refuses the device as well: the step must produce final values of one aggregation.
    if (!aggregating->isFinal() || aggregating->isGroupingSets() || aggregating->isGroupByUseNulls() || aggregating->isMergingSkipped()
        || !aggregating->getGroupBySortDescription().empty() || aggregating->explicitSortingRequired())
        return;

    const SharedHeader input_header = aggregating->getInputHeaders().front();
    if (!GPUAggregatingStep::canRunOnDevice(*input_header, aggregating->getParams()))
        return;

    auto on_device = std::make_unique<GPUAggregatingStep>(input_header, aggregating->getParams(), settings.gpu_aggregation_batch_bytes);
    if (!blocksHaveEqualStructure(*on_device->getOutputHeader(), *aggregating->getOutputHeader()))
        return;

    LOG_DEBUG(getLogger("GPUAggregating"), "Aggregating on the device an aggregation made after the planner: {}", aggregating->getStepDescription());
    on_device->setStepDescription(*aggregating);
    node.step = std::move(on_device);
}

}

void optimizeAggregationOnGPU(QueryPlan::Node & node, const QueryPlanOptimizationSettings & settings)
{
    replaceWithGPUAggregating(node, settings);

    /// The plan is optimized from the top down, so the aggregation below a `HAVING` is reached from the filter.
    if (const auto * filter = typeid_cast<const FilterStep *>(node.step.get()); filter && node.children.size() == 1)
        replaceWithGPUAggregating(*node.children.front(), settings);
}

}

#else

namespace DB::QueryPlanOptimizations
{

void optimizeAggregationOnGPU(QueryPlan::Node &, const QueryPlanOptimizationSettings &)
{
}

}

#endif
