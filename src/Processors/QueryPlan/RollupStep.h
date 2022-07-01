#pragma once
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/// WITH ROLLUP. See RollupTransform.
class RollupStep : public ITransformingStep
{
public:
    RollupStep(const DataStream & input_stream_, Aggregator::Params params_, bool final_, bool use_nulls_);

    String getName() const override { return "Rollup"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputStream() override;

    Aggregator::Params params;
    size_t keys_size;
    bool final;
    bool use_nulls;
};

}
