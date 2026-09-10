#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

namespace DB
{

struct NegativeLimitWire;

/// Executes Negative LIMIT. See NegativeLimitTransform.
class NegativeLimitStep : public ITransformingStep
{
public:
    NegativeLimitStep(
        const SharedHeader & input_header_,
        UInt64 limit_, UInt64 offset_,
        bool with_ties_ = false,
        SortDescription description_ = {});

    String getName() const override { return "NegativeLimit"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    UInt64 getLimit() const { return limit; }

    void markAsShardLimit() { is_shard_limit = true; }

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `NegativeLimitStep.cpp` declares.
    NegativeLimitWire toWire() const;
    static QueryPlanStepPtr fromWire(NegativeLimitWire wire, Deserialization & ctx);

    bool hasCorrelatedExpressions() const override { return false; }

    /// `LIMIT -n` returns the *last* `n` rows, and `addPreliminaryLimitStep` can push it to the shard, so
    /// this step can be the replica-output boundary. It is a top-N taken from the tail, so like
    /// `LimitStep` its output is replicated: every replica ships its own last `n` rows.
    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    UInt64 limit;
    UInt64 offset;
    bool with_ties;
    const SortDescription description;
    bool is_shard_limit = false;
};

/// What `NegativeLimitStep` puts on the wire in the framed format.
struct NegativeLimitWire
{
    UInt64 limit = 0;
    UInt64 offset = 0;
    bool with_ties = false;
    /// Empty unless `with_ties`; one byte on the wire when empty.
    SortDescription description;
    /// Reaches the transform, so the receiver needs it to build the same pipeline.
    bool is_shard_limit = false;

    bool operator==(const NegativeLimitWire &) const = default;
};

}
