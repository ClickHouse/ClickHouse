#pragma once
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

struct LimitByWire;

/// Executes LIMIT BY for specified columns. See LimitByTransform.
class LimitByStep : public ITransformingStep
{
public:
    explicit LimitByStep(
            const SharedHeader & input_header_,
            size_t group_length_, size_t group_offset_, Names columns_);

    String getName() const override { return "LimitBy"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `LimitByStep.cpp` declares.
    LimitByWire toWire() const;
    static QueryPlanStepPtr fromWire(LimitByWire wire, Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    size_t getGroupLength() const { return group_length; }
    size_t getGroupOffset() const { return group_offset; }
    const Names & getColumns() const { return columns; }

    void applyOrder(const SortDescription & sort_description);

    /// Skip the resize-to-one-stream and run one `LimitByTransform` per input stream.
    /// Set by `optimizeLimitByPerPartition`; assumes upstream streams carry disjoint
    /// partition sets so no `LIMIT BY` group spans two streams.
    void skipStreamMerging() { skip_stream_merging = true; }

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    size_t group_length;
    size_t group_offset;

    Names columns;

    SortDescription sorted_columns_descr;

    bool skip_stream_merging = false;
};

/// What `LimitByStep` puts on the wire in the framed format.
struct LimitByWire
{
    UInt64 group_length = 0;
    UInt64 group_offset = 0;
    Names columns;
    /// Selects the sorted-stream transform, which is correct only for an input sorted this way.
    SortDescription sorted_columns_descr;
    /// Lets the step skip the merge into one stream.
    bool skip_stream_merging = false;

    bool operator==(const LimitByWire &) const = default;
};

}
