#include <Storages/MergeTree/Streaming/ReadingPlan/CalculatePartitionWatermarks.h>

#include <Processors/Streaming/CalculateWatermarksTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Streaming/Utils.h>

#include <Core/Block.h>

namespace DB
{

namespace
{

class CalculatePartitionWatermarksTransform final : public CalculateWatermarksTransform
{
public:
    CalculatePartitionWatermarksTransform(
        SharedHeader input_header_,
        SharedHeader output_header_,
        std::string event_time_column_,
        ActionsDAG watermark_expression_,
        ContextPtr context_,
        String partition_id_)
        : CalculateWatermarksTransform(std::move(input_header_), std::move(output_header_), std::move(event_time_column_), std::move(watermark_expression_), std::move(context_))
        , partition_id(std::move(partition_id_))
    {
    }

    String getName() const override { return "CalculatePartitionWatermarks"; }

protected:
    void transformChunk(Chunk & chunk, const Field & watermark) override
    {
        auto info = std::make_shared<PartitionWatermarkInfo>();
        info->partition_id = partition_id;
        info->watermark = watermark;
        chunk.getChunkInfos().add(std::move(info));
    }

private:
    const String partition_id;
};

}

CalculatePartitionWatermarksStep::CalculatePartitionWatermarksStep(
    SharedHeader input_header_, WatermarkSettingsPtr watermark_, ContextPtr context_, String partition_id_)
    : CalculateWatermarksStep(std::move(input_header_), std::move(watermark_), std::move(context_))
    , partition_id(std::move(partition_id_))
{
}

void CalculatePartitionWatermarksStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&] (const SharedHeader & header)
    {
        auto watermark_expression = buildWatermarkActionsDAG(watermark->expression, *input_headers.front(), context);
        return std::make_shared<CalculatePartitionWatermarksTransform>(header, getOutputHeader(), watermark->column, std::move(watermark_expression), context, partition_id);
    });
}

QueryPlanStepPtr CalculatePartitionWatermarksStep::clone() const
{
    return std::make_unique<CalculatePartitionWatermarksStep>(input_headers.front(), watermark, context, partition_id);
}

}
