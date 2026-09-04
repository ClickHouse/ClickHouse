#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
namespace DB
{

struct ExtremesWire;

/// Calculate extremes. Add special port for extremes.
class ExtremesStep : public ITransformingStep
{
public:
    explicit ExtremesStep(const SharedHeader & input_header_);

    String getName() const override { return "Extremes"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `ExtremesStep.cpp` declares.
    ExtremesWire toWire() const;
    static QueryPlanStepPtr fromWire(ExtremesWire wire, Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }
};

/// `ExtremesStep` has no payload: the output header is its whole state.
struct ExtremesWire
{
    bool operator==(const ExtremesWire &) const = default;
};

}
