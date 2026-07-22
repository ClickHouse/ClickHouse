#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Serializes and compresses input blocks to avoid doing it on TCPHandler.
class BlocksMarshallingStep : public ITransformingStep
{
public:
    explicit BlocksMarshallingStep(const SharedHeader & input_header_);

    String getName() const override { return "BlocksMarshalling"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization &) const override { }
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override;
};

}
