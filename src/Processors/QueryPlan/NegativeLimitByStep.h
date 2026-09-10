#pragma once
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

struct NegativeLimitByWire;

/// Executes negative LIMIT BY for specified columns. See NegativeLimitByTransform.
class NegativeLimitByStep : public ITransformingStep
{
public:
    explicit NegativeLimitByStep(
            const SharedHeader & input_header_,
            size_t group_length_, size_t group_offset_, Names columns_);

    String getName() const override { return "NegativeLimitBy"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `NegativeLimitByStep.cpp` declares.
    NegativeLimitByWire toWire() const;
    static QueryPlanStepPtr fromWire(NegativeLimitByWire wire, Deserialization & ctx);

    const Names & getColumns() const { return columns; }

    void applyOrder(const SortDescription & sort_description);

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
};

/// What `NegativeLimitByStep` puts on the wire in the framed format.
struct NegativeLimitByWire
{
    UInt64 group_length = 0;
    UInt64 group_offset = 0;
    Names columns;
    /// Selects the sorted-stream transform, which is correct only for an input sorted this way.
    SortDescription sorted_columns_descr;

    bool operator==(const NegativeLimitByWire &) const = default;
};

}
