#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

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

    size_t getGroupLength() const { return group_length; }
    size_t getGroupOffset() const { return group_offset; }
    const Names & getColumns() const { return columns; }

    void applyOrder(bool in_order_) { in_order = in_order_; }

    /// Skip the resize-to-one-stream and run one `LimitByTransform` per input stream.
    /// Set by `optimizeLimitByPerPartition`; assumes upstream streams carry disjoint
    /// partition sets so no `LIMIT BY` group spans two streams.
    void skipStreamMerging() { skip_stream_merging = true; }

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    size_t group_length;
    size_t group_offset;

    Names columns;

    bool in_order = false;
    bool skip_stream_merging = false;
};

}
