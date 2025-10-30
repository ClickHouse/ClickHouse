#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{
/// Execute DISTINCT for specified columns in parallel manner.
class PartitionedDistinctStep : public ITransformingStep
{
public:
    PartitionedDistinctStep(
        const SharedHeader & input_header_,
        const Names & columns_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        size_t partitions_num_);

    String getName() const override { return "PartitionedDistinct"; }

    String getSerializationName() const override { return "PartitionedDistinct"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    UInt64 getLimitHint() const { return limit_hint; }
    void updateLimitHint(UInt64 hint);

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    const SizeLimits & getSetSizeLimits() const { return set_size_limits; }

private:
    SizeLimits set_size_limits;
    UInt64 limit_hint;
    const Names columns;
    size_t partitions_num;

    void updateOutputHeader() override;
};
}
