#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/Aggregator.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/// WITH CUBE. See CubeTransform.
class CubeStep : public ITransformingStep
{
public:
    CubeStep(const DataStream & input_stream_, Aggregator::Params params_, bool final_);

    String getName() const override { return "Cube"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    const Aggregator::Params & getParams() const;
private:
    void updateOutputStream() override;

    size_t keys_size;
    Aggregator::Params params;
    bool final;
};

}
