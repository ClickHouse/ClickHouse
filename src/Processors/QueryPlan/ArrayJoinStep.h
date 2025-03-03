#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/ArrayJoin.h>

namespace DB
{

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

class ArrayJoinStep : public ITransformingStep
{
public:
    ArrayJoinStep(const Header & input_header_, ArrayJoin array_join_, bool is_unaligned_, size_t max_block_size_);
    String getName() const override { return "ArrayJoin"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const Names & getColumns() const { return array_join.columns; }
    bool isLeft() const { return array_join.is_left; }

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override;

    ArrayJoin array_join;
    bool is_unaligned = false;
    size_t max_block_size = DEFAULT_BLOCK_SIZE;
};

}
