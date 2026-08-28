#include <Storages/MergeTree/Streaming/ReadRound/StampPartitionWatermarks.h>

#include <Processors/Streaming/Markers.h>
#include <Processors/IInflatingTransform.h>
#include <Processors/Port.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Core/Block.h>
#include <Core/Streaming/StreamingVirtualColumns.h>

#include <Columns/IColumn.h>

#include <base/defines.h>

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

std::shared_ptr<PartitionWatermarkInfo> createPartitionWatermarkInfo(const String & partition_id, const Field & watermark)
{
    auto info = std::make_shared<PartitionWatermarkInfo>();
    info->partition_id = partition_id;
    info->watermark = watermark;
    return info;
}

class StampPartitionWatermarksTransform final : public IInflatingTransform
{
public:
    StampPartitionWatermarksTransform(SharedHeader header, String partition_id_)
        : IInflatingTransform(header, header)
        , watermark_column_pos(header->getPositionByName(WatermarkColumn::name))
        , partition_id(std::move(partition_id_))
    {
    }

    String getName() const override { return "StampPartitionWatermarks"; }

    void consume(Chunk chunk) override
    {
        if (auto marker = chunk.getChunkInfos().get<WatermarkMarker>())
        {
            watermark = marker->watermark;
            Chunk info_chunk(getOutputPort().getHeader().cloneEmptyColumns(), 0);
            info_chunk.getChunkInfos().add(createPartitionWatermarkInfo(partition_id, watermark));
            pending_chunks.push(std::move(info_chunk));
        }
        else if (const size_t num_rows = chunk.getNumRows(); num_rows > 0)
        {
            chunk.getColumns()[watermark_column_pos]->get(num_rows - 1, watermark);
            chunk.getChunkInfos().add(createPartitionWatermarkInfo(partition_id, watermark));
        }

        pending_chunks.push(std::move(chunk));
    }

    bool canGenerate() override
    {
        return !pending_chunks.empty();
    }

    Chunk generate() override
    {
        chassert(!pending_chunks.empty());
        auto chunk = std::move(pending_chunks.front());
        pending_chunks.pop();
        return chunk;
    }

private:
    const size_t watermark_column_pos;
    const String partition_id;

    Field watermark;
    std::queue<Chunk> pending_chunks;
};

}

StampPartitionWatermarksStep::StampPartitionWatermarksStep(SharedHeader input_header_, String partition_id_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , partition_id(std::move(partition_id_))
{
}

void StampPartitionWatermarksStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

void StampPartitionWatermarksStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&] (const SharedHeader & header)
    {
        return std::make_shared<StampPartitionWatermarksTransform>(header, partition_id);
    });
}

QueryPlanStepPtr StampPartitionWatermarksStep::clone() const
{
    return std::make_unique<StampPartitionWatermarksStep>(input_headers.front(), partition_id);
}

}
