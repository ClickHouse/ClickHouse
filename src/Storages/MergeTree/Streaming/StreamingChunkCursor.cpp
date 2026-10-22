#include <Storages/MergeTree/Streaming/StreamingChunkCursor.h>

#include <Columns/IColumn.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <base/defines.h>

namespace DB
{

namespace
{

ITransformingStep::Traits getCursorBuildTraits()
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

/// It is assumed that chunk was originated from a single partition.
class BuildStreamingChunkCursorTransform : public ISimpleTransform
{
public:
    explicit BuildStreamingChunkCursorTransform(SharedHeader header_)
        : ISimpleTransform(header_, header_, /*skip_empty_chunks=*/false)
        , pos_partition_id(header_->getPositionByName(PartitionIdColumn::name))
        , pos_block_number(header_->getPositionByName(BlockNumberColumn::name))
        , pos_block_offset(header_->getPositionByName(BlockOffsetColumn::name))
    {
    }

    String getName() const override { return "BuildStreamingChunkCursor"; }

    void transform(Chunk & chunk) override
    {
        const size_t rows = chunk.getNumRows();
        if (rows == 0)
            return;

        const auto & cols = chunk.getColumns();

        auto info = std::make_shared<StreamingChunkCursorInfo>();
        info->partition_id = String(cols[pos_partition_id]->getDataAt(0));
        info->last_block_number = cols[pos_block_number]->getInt(rows - 1);
        info->last_block_offset = cols[pos_block_offset]->getInt(rows - 1);

        chunk.getChunkInfos().add(std::move(info));
    }

private:
    const size_t pos_partition_id;
    const size_t pos_block_number;
    const size_t pos_block_offset;
};

}

BuildStreamingChunkCursorStep::BuildStreamingChunkCursorStep(SharedHeader input_header_)
    : ITransformingStep(input_header_, input_header_, getCursorBuildTraits())
{
}

void BuildStreamingChunkCursorStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([](const SharedHeader & header)
    {
        return std::make_shared<BuildStreamingChunkCursorTransform>(header);
    });
}

void BuildStreamingChunkCursorStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

QueryPlanStepPtr BuildStreamingChunkCursorStep::clone() const
{
    return std::make_unique<BuildStreamingChunkCursorStep>(*this);
}

}
