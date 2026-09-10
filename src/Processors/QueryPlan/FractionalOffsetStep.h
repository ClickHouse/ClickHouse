#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <base/types.h>

namespace DB
{

struct FractionalOffsetWire;

/// Executes Fractional OFFSET (without LIMIT). See FractionalOffsetTransform.
class FractionalOffsetStep : public ITransformingStep
{
public:
    FractionalOffsetStep(const SharedHeader & input_header_, Float64 fractional_offset_);

    String getName() const override { return "FractionalOffset"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `FractionalOffsetStep.cpp` declares.
    FractionalOffsetWire toWire() const;
    static QueryPlanStepPtr fromWire(FractionalOffsetWire wire, Deserialization & ctx);
    /// Like `FractionalLimitStep`: the fraction is resolved against the whole result.
    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    void updateOutputHeader() override { output_header = input_headers.front(); }

    Float64 fractional_offset;
};

/// What `FractionalOffsetStep` puts on the wire in the framed format.
struct FractionalOffsetWire
{
    Float64 fractional_offset = 0;

    bool operator==(const FractionalOffsetWire &) const = default;
};

}
