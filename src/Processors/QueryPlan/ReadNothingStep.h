#pragma once
#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

struct ReadNothingWire;

/// Create NullSource with specified structure.
class ReadNothingStep : public ISourceStep
{
public:
    explicit ReadNothingStep(SharedHeader output_header);

    String getName() const override { return "ReadNothing"; }

    QueryPlanStepPtr clone() const override;

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `ReadNothingStep.cpp` declares.
    ReadNothingWire toWire() const;
    static QueryPlanStepPtr fromWire(ReadNothingWire wire, Deserialization & ctx);

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
};

/// `ReadNothingStep` has no payload: the output header is its whole state.
struct ReadNothingWire
{
    bool operator==(const ReadNothingWire &) const = default;
};

}
