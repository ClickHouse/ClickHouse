#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/PromQLRangeTopKByTransform.h>


namespace DB
{

/// Applies bounded `topk`/`bottomk` selection after the native range-sum
/// kernel has emitted one merged row per projected group.
class PromQLRangeTopKByStep final : public ITransformingStep
{
public:
    PromQLRangeTopKByStep(SharedHeader input_header_, UInt64 k_, bool bottomk_);

    String getName() const override { return "PromQLRangeTopKBy"; }
    bool isInputOrderDependent() const override { return false; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputHeader() override;

    const UInt64 k;
    const bool bottomk;
};

}
