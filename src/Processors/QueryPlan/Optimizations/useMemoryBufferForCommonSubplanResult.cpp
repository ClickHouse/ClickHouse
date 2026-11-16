
#include <Processors/ChunkBuffer.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromCommonBufferStep.h>
#include <Processors/QueryPlan/SaveSubqueryResultToBufferStep.h>


namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

namespace QueryPlanOptimizations
{

void useMemoryBufferForCommonSubplanResult(QueryPlan::Node & node, const QueryPlanOptimizationSettings & settings)
{
    auto * subplan_reference = typeid_cast<CommonSubplanReferenceStep *>(node.step.get());
    if (!subplan_reference)
        return;

    auto * subplan_reference_root = subplan_reference->getSubplanReferenceRoot();
    auto * common_subplan = typeid_cast<CommonSubplanStep *>(subplan_reference_root->step.get());
    if (!common_subplan)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected CommonSubplanReferenceStep to reference CommonSubplanStep, but got {}",
            node.step->getName());

    auto columns_to_use = subplan_reference->extractColumnsToUse();

    auto common_buffer = std::make_shared<ChunkBuffer>();
    node.step = std::make_unique<ReadFromCommonBufferStep>(
        subplan_reference->getOutputHeader(),
        common_buffer,
        settings.max_threads);
    subplan_reference_root->step = std::make_unique<SaveSubqueryResultToBufferStep>(
        subplan_reference_root->step->getOutputHeader(),
        std::move(columns_to_use),
        common_buffer);
}

}

}
