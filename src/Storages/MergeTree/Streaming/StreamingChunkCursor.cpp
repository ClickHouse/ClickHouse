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

ITransformingStep::Traits getCursorBuildTraits(bool unordered)
{
    return ITransformingStep::Traits
    {
        .data_stream_traits = {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = !unordered,
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
    BuildStreamingChunkCursorTransform(SharedHeader header_, bool unordered_)
        : ISimpleTransform(header_, header_, /*skip_empty_chunks=*/false)
        , unordered(unordered_)
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

        if (unordered)
        {
            /// Rows are not sorted by cursor; take the maximum (_block_number, _block_offset) in the chunk.
            Int64 max_block_number = cols[pos_block_number]->getInt(0);
            Int64 max_block_offset = cols[pos_block_offset]->getInt(0);
            for (size_t i = 1; i < rows; ++i)
            {
                const Int64 block_number = cols[pos_block_number]->getInt(i);
                const Int64 block_offset = cols[pos_block_offset]->getInt(i);
                if (block_number > max_block_number || (block_number == max_block_number && block_offset > max_block_offset))
                {
                    max_block_number = block_number;
                    max_block_offset = block_offset;
                }
            }
            info->last_block_number = max_block_number;
            info->last_block_offset = max_block_offset;
        }
        else
        {
            info->last_block_number = cols[pos_block_number]->getInt(rows - 1);
            info->last_block_offset = cols[pos_block_offset]->getInt(rows - 1);
        }

        chunk.getChunkInfos().add(std::move(info));
    }

private:
    const bool unordered;
    const size_t pos_partition_id;
    const size_t pos_block_number;
    const size_t pos_block_offset;
};

}

BuildStreamingChunkCursorStep::BuildStreamingChunkCursorStep(SharedHeader input_header_, bool unordered_)
    : ITransformingStep(input_header_, input_header_, getCursorBuildTraits(unordered_))
    , unordered(unordered_)
{
}

void BuildStreamingChunkCursorStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([is_unordered = unordered](const SharedHeader & header)
    {
        return std::make_shared<BuildStreamingChunkCursorTransform>(header, is_unordered);
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
