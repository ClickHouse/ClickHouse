#include <Storages/MergeTree/Streaming/SavePartitionWatermark.h>

#include <Processors/IInflatingTransform.h>
#include <Processors/Port.h>
#include <Processors/Streaming/MarkerWatermark.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Core/Block.h>

#include <queue>

namespace DB
{

namespace
{

ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        .data_stream_traits = {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        .transform_traits = {
            .preserves_number_of_rows = true,
        },
    };
}

class SavePartitionWatermarkTransform : public IInflatingTransform
{
public:
    SavePartitionWatermarkTransform(SharedHeader header, String partition_id_)
        : IInflatingTransform(header, header)
        , partition_id(std::move(partition_id_))
    {
    }

    String getName() const override { return "SavePartitionWatermark"; }

protected:
    void consume(Chunk chunk) override
    {
        std::optional<Field> watermark;
        if (auto marker = chunk.getChunkInfos().get<WatermarkMarker>())
            watermark = marker->watermark;

        pending_chunks.push(std::move(chunk));

        if (watermark.has_value())
        {
            Chunk info_chunk(getInputPort().getHeader().cloneEmptyColumns(), 0);
            info_chunk.getChunkInfos().add(std::make_shared<PartitionWatermarkInfo>(partition_id, std::move(*watermark)));
            pending_chunks.push(std::move(info_chunk));
        }
    }

    bool canGenerate() override { return !pending_chunks.empty(); }

    Chunk generate() override
    {
        chassert(!pending_chunks.empty());
        auto chunk = std::move(pending_chunks.front());
        pending_chunks.pop();
        return chunk;
    }

private:
    const String partition_id;
    std::queue<Chunk> pending_chunks;
};

}

void SavePartitionWatermarkStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

SavePartitionWatermarkStep::SavePartitionWatermarkStep(SharedHeader input_header_, String partition_id_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , partition_id(std::move(partition_id_))
{
}

void SavePartitionWatermarkStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&] (const SharedHeader & header)
    {
        return std::make_shared<SavePartitionWatermarkTransform>(header, partition_id);
    });
}

QueryPlanStepPtr SavePartitionWatermarkStep::clone() const
{
    return std::make_unique<SavePartitionWatermarkStep>(input_headers.front(), partition_id);
}

}
