#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

namespace DB
{

struct LimitWire;

/// Executes LIMIT. See LimitTransform.
class LimitStep : public ITransformingStep
{
public:
    LimitStep(
        const SharedHeader & input_header_,
        size_t limit_, size_t offset_,
        bool always_read_till_end_ = false, /// Read all data even if limit is reached. Needed for totals.
        bool with_ties_ = false, /// Limit with ties.
        SortDescription description_ = {});

    String getName() const override { return "Limit"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    size_t getLimit() const { return limit; }
    size_t getOffset() const { return offset; }

    size_t getLimitForSorting() const
    {
        if (limit > std::numeric_limits<UInt64>::max() - offset)
            return 0;

        return limit + offset;
    }

    bool withTies() const { return with_ties; }
    bool alwaysReadTillEnd() const { return always_read_till_end; }

    void markAsShardLimit() { is_shard_limit = true; }

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `LimitStep.cpp` declares.
    LimitWire toWire() const;
    static QueryPlanStepPtr fromWire(LimitWire wire, Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override { return false; }

    /// A `Limit` at the replica-output boundary is a shard limit, so its output is replicated, not
    /// partitioned: every replica emits up to `limit` rows and ships all of them.
    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);

    size_t limit;
    size_t offset;
    bool always_read_till_end;

    bool with_ties;
    const SortDescription description;
    bool is_shard_limit = false;
};

/// What `LimitStep` puts on the wire in the framed format.
struct LimitWire
{
    UInt64 limit = 0;
    UInt64 offset = 0;
    bool always_read_till_end = false;
    bool with_ties = false;
    /// Empty unless `with_ties`; one byte on the wire when empty.
    SortDescription description;
    /// Reaches the transform, so the receiver needs it to build the same pipeline.
    bool is_shard_limit = false;

    bool operator==(const LimitWire &) const = default;
};

}
