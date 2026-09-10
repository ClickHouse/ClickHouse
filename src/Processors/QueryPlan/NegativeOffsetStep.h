#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

struct NegativeOffsetWire;

/// Executes OFFSET (without LIMIT). See OffsetTransform.
class NegativeOffsetStep : public ITransformingStep
{
public:
    NegativeOffsetStep(const SharedHeader & input_header_, UInt64 offset_);

    String getName() const override { return "NegativeOffset"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `NegativeOffsetStep.cpp` declares.
    NegativeOffsetWire toWire() const;
    static QueryPlanStepPtr fromWire(NegativeOffsetWire wire, Deserialization & ctx);
    /// Like `OffsetStep`: a negative `OFFSET` applies to the whole result, so it runs on the initiator.
    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    void updateOutputHeader() override { output_header = input_headers.front(); }

    UInt64 offset;
};

/// What `NegativeOffsetStep` puts on the wire in the framed format.
struct NegativeOffsetWire
{
    UInt64 offset = 0;

    bool operator==(const NegativeOffsetWire &) const = default;
};

}
