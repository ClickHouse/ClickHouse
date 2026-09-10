#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

struct OffsetWire;

/// Executes OFFSET (without LIMIT). See OffsetTransform.
class OffsetStep : public ITransformingStep
{
public:
    OffsetStep(const SharedHeader & input_header_, size_t offset_);

    String getName() const override { return "Offset"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `OffsetStep.cpp` declares.
    OffsetWire toWire() const;
    static QueryPlanStepPtr fromWire(OffsetWire wire, Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    /// `OFFSET` skips a prefix of the whole result and cannot be evaluated per replica, so it runs on
    /// the initiator; where the planner does put one on a shard (under a negative limit) it is never the
    /// topmost replica step.
    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);

    size_t offset;
};

/// What `OffsetStep` puts on the wire in the framed format.
struct OffsetWire
{
    UInt64 offset = 0;

    bool operator==(const OffsetWire &) const = default;
};

}
