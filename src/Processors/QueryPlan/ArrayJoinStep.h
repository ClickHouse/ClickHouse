#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

class ArrayJoinStep : public ITransformingStep
{
public:
    ArrayJoinStep(const DataStream & input_stream_, NameSet columns_, bool is_left_, bool is_unaligned_, size_t max_block_size_);
    String getName() const override { return "ArrayJoin"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const NameSet & getColumns() const { return columns; }
    bool isLeft() const { return is_left; }

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(WriteBuffer & out) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(ReadBuffer & in, const DataStreams & input_streams_, const DataStream *, QueryPlanSerializationSettings & settings);

private:
    void updateOutputStream() override;

    NameSet columns;
    bool is_left;
    bool is_unaligned;
    size_t max_block_size;
};

}
