#include <Storages/MergeTree/Streaming/StreamingChunkCursor.h>

#include <Columns/IColumn.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <base/defines.h>

#include <algorithm>

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

/// Sets StreamingChunkCursorInfo for each chunk; the cursor is computed by the derived class.
/// It is assumed that a chunk originates from a single partition.
class BuildStreamingChunkCursorTransformBase : public ISimpleTransform
{
public:
    explicit BuildStreamingChunkCursorTransformBase(SharedHeader header_)
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
        info->cursor = computeChunkCursor(cols, rows);

        chunk.getChunkInfos().add(std::move(info));
    }

protected:
    PartitionCursor cursorAt(const Columns & cols, size_t row) const
    {
        return {cols[pos_block_number]->getInt(row), cols[pos_block_offset]->getInt(row)};
    }

private:
    virtual PartitionCursor computeChunkCursor(const Columns & cols, size_t rows) const = 0;

    const size_t pos_partition_id;
    const size_t pos_block_number;
    const size_t pos_block_offset;
};

/// Ordered stream: rows are already sorted by cursor, so the last row carries the chunk's cursor.
class BuildStreamingChunkCursorTransform : public BuildStreamingChunkCursorTransformBase
{
public:
    using BuildStreamingChunkCursorTransformBase::BuildStreamingChunkCursorTransformBase;

private:
    PartitionCursor computeChunkCursor(const Columns & cols, size_t rows) const override
    {
        return cursorAt(cols, rows - 1);
    }
};

/// Unordered stream: rows are not sorted by cursor, so take the maximum cursor in the chunk.
class BuildUnorderedStreamingChunkCursorTransform : public BuildStreamingChunkCursorTransformBase
{
public:
    using BuildStreamingChunkCursorTransformBase::BuildStreamingChunkCursorTransformBase;

private:
    PartitionCursor computeChunkCursor(const Columns & cols, size_t rows) const override
    {
        PartitionCursor max_cursor = cursorAt(cols, 0);
        for (size_t i = 1; i < rows; ++i)
            max_cursor = std::max(max_cursor, cursorAt(cols, i));
        return max_cursor;
    }
};

}

BuildStreamingChunkCursorStep::BuildStreamingChunkCursorStep(SharedHeader input_header_, bool unordered_)
    : ITransformingStep(input_header_, input_header_, getCursorBuildTraits(unordered_))
    , unordered(unordered_)
{
}

void BuildStreamingChunkCursorStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([is_unordered = unordered](const SharedHeader & header) -> ProcessorPtr
    {
        if (is_unordered)
            return std::make_shared<BuildUnorderedStreamingChunkCursorTransform>(header);
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
