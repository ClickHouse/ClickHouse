#include <Storages/MergeTree/Streaming/ReadingPlan/StampPartitionCursors.h>

#include <Columns/IColumn.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <base/defines.h>

#include <algorithm>
#include <tuple>
#include <utility>

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

class StampPartitionCursorsTransform final : public ISimpleTransform
{
    PartitionCursor cursorAt(const Columns & cols, size_t row) const
    {
        chassert(cols[pos_block_number]->size() == cols[pos_block_offset]->size());
        chassert(row < cols[pos_block_number]->size() && row < cols[pos_block_offset]->size());
        return {cols[pos_block_number]->getInt(row), cols[pos_block_offset]->getInt(row)};
    }

    std::pair<PartitionCursor, PartitionCursor> orderedRange(const Columns & cols, size_t rows) const
    {
        return {cursorAt(cols, 0), cursorAt(cols, rows - 1)};
    }

    std::pair<PartitionCursor, PartitionCursor> unorderedRange(const Columns & cols, size_t rows) const
    {
        PartitionCursor min_cursor = cursorAt(cols, 0);
        PartitionCursor max_cursor = min_cursor;
        for (size_t i = 1; i < rows; ++i)
        {
            const auto cursor = cursorAt(cols, i);
            min_cursor = std::min(min_cursor, cursor);
            max_cursor = std::max(max_cursor, cursor);
        }
        return {std::move(min_cursor), std::move(max_cursor)};
    }

public:
    StampPartitionCursorsTransform(SharedHeader header_, String partition_id_, bool unordered_)
        : ISimpleTransform(header_, header_, /*skip_empty_chunks=*/false)
        , partition_id(std::move(partition_id_))
        , unordered(unordered_)
        , pos_block_number(header_->getPositionByName(BlockNumberColumn::name))
        , pos_block_offset(header_->getPositionByName(BlockOffsetColumn::name))
    {
    }

    String getName() const override { return "StampPartitionCursors"; }

    void transform(Chunk & chunk) override
    {
        const auto & cols = chunk.getColumns();
        const size_t rows = chunk.getNumRows();
        if (rows == 0)
            return;

        auto info = std::make_shared<PartitionCursorInfo>();
        info->partition_id = partition_id;
        std::tie(info->first, info->last) = unordered ? unorderedRange(cols, rows) : orderedRange(cols, rows);

        chunk.getChunkInfos().add(std::move(info));
    }

private:
    const String partition_id;
    const bool unordered;
    const size_t pos_block_number;
    const size_t pos_block_offset;
};

}

StampPartitionCursorsStep::StampPartitionCursorsStep(SharedHeader input_header_, String partition_id_, bool unordered_)
    : ITransformingStep(input_header_, input_header_, getCursorBuildTraits(unordered_))
    , partition_id(std::move(partition_id_))
    , unordered(unordered_)
{
}

void StampPartitionCursorsStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([this](const SharedHeader & header) -> ProcessorPtr
    {
        return std::make_shared<StampPartitionCursorsTransform>(header, partition_id, unordered);
    });
}

void StampPartitionCursorsStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

QueryPlanStepPtr StampPartitionCursorsStep::clone() const
{
    return std::make_unique<StampPartitionCursorsStep>(*this);
}

}
