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
    /// `key_positions_` maps each element of the GROUP BY list, in order and keeping repetitions,
    /// onto its index in `params_.keys`, which is deduplicated. Empty means no key was repeated.
    CubeStep(const SharedHeader & input_header_, Aggregator::Params params_, bool final_, bool use_nulls_,
             std::vector<size_t> key_positions_ = {});

    String getName() const override { return "Cube"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    const Aggregator::Params & getParams() const;

    QueryPlanStepPtr clone() const override;

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);
    bool isSerializable() const override { return true; }
private:
    void updateOutputHeader() override;

    size_t keys_size;
    Aggregator::Params params;
    std::vector<size_t> key_positions;
    bool final;
    bool use_nulls;
};

}
