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
    CubeStep(const SharedHeader & input_header_, Aggregator::Params params_, bool final_, bool use_nulls_);

    [[nodiscard]] String getName() const override { return "Cube"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    [[nodiscard]] const Aggregator::Params & getParams() const;

    [[nodiscard]] QueryPlanStepPtr clone() const override;

    /// the filter push-down copies a filter below the step only one time
    [[nodiscard]] bool isFilterPushedDown() const { return filter_pushed_down; }
    void setFilterPushedDown() { filter_pushed_down = true; }

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);
    [[nodiscard]] bool isSerializable() const override { return true; }
private:
    void updateOutputHeader() override;

    size_t keys_size;
    Aggregator::Params params;
    bool final;
    bool use_nulls;
    bool filter_pushed_down = false;
};

}
