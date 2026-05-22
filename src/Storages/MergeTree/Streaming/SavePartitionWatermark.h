#pragma once

#include <Core/Field.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/ITransformingStep.h>

#include <base/types.h>

namespace DB
{

struct PartitionWatermarkInfo : public ChunkInfoCloneable<PartitionWatermarkInfo>
{
    String partition_id;
    Field watermark;

    PartitionWatermarkInfo(String partition_id_, Field watermark_)
        : partition_id(std::move(partition_id_))
        , watermark(std::move(watermark_))
    {
    }
};

class SavePartitionWatermarkStep : public ITransformingStep
{
    void updateOutputHeader() override;

public:
    SavePartitionWatermarkStep(SharedHeader input_header_, String partition_id_);

    String getName() const override { return "SavePartitionWatermark"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    QueryPlanStepPtr clone() const override;

private:
    const String partition_id;
};

}
