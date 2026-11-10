#include <Storages/MergeTree/Streaming/ReadingPlan/StampPartitionWatermarks.h>

#include <Processors/Streaming/Markers.h>
#include <Processors/IInflatingTransform.h>
#include <Processors/Port.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Core/Block.h>

#include <base/defines.h>

#include <optional>
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

class StampPartitionWatermarksTransform : public IInflatingTransform
{
public:
    StampPartitionWatermarksTransform(SharedHeader header, String partition_id_)
        : IInflatingTransform(header, header)
        , partition_id(std::move(partition_id_))
    {
    }

    String getName() const override { return "StampPartitionWatermarks"; }

protected:
    void consume(Chunk chunk) override
    {
        std::optional<Field> watermark;
        if (auto marker = chunk.getChunkInfos().get<WatermarkMarker>())
            watermark = marker->watermark;

        pending_chunks.push(std::move(chunk));

        if (watermark)
        {
            auto partition_marker = std::make_shared<PartitionWatermarkInfo>();
            partition_marker->partition_id = partition_id;
            partition_marker->watermark = std::move(*watermark);

            Chunk partition_marker_chunk(getOutputPort().getHeader().cloneEmptyColumns(), 0);
            partition_marker_chunk.getChunkInfos().add(std::move(partition_marker));
            pending_chunks.push(std::move(partition_marker_chunk));
        }
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
    const String partition_id;
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
