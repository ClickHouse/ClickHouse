#include <Storages/MergeTree/Streaming/ReadingPlan/StampPartitionWatermarks.h>

#include <Processors/Streaming/Markers.h>
#include <Processors/ISimpleTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Core/Block.h>

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

class StampPartitionWatermarksTransform : public ISimpleTransform
{
public:
    StampPartitionWatermarksTransform(SharedHeader header, String partition_id_)
        : ISimpleTransform(header, header, /*skip_empty_chunks_=*/false)
        , partition_id(std::move(partition_id_))
    {
    }

    String getName() const override { return "StampPartitionWatermarks"; }

protected:
    void transform(Chunk & chunk) override
    {
        auto marker = chunk.getChunkInfos().get<WatermarkMarker>();
        if (!marker)
            return;

        auto partition_marker = std::make_shared<PartitionWatermarkInfo>();
        partition_marker->partition_id = partition_id;
        partition_marker->watermark = marker->watermark;
        chunk.getChunkInfos().add(std::move(partition_marker));
    }

private:
    const String partition_id;
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
