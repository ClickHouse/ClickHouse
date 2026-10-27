#include <Storages/MergeTree/Streaming/ReadingPlan/AlignStreams.h>
#include <Storages/MergeTree/Streaming/ReadingPlan/StampPartitionCursors.h>
#include <Storages/MergeTree/Streaming/Cursors/CursorUtils.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <Columns/IColumn.h>

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/Streaming/CalculateWatermarksTransform.h>
#include <Processors/Streaming/Markers.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Core/Block.h>
#include <Core/Field.h>

#include <Common/Exception.h>

#include <algorithm>
#include <deque>
#include <iterator>
#include <optional>
#include <queue>
#include <ranges>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

InputPorts buildInputPorts(SharedHeader metadata_header, SharedHeader data_header)
{
    InputPorts ports;
    ports.emplace_back(std::move(metadata_header));
    ports.emplace_back(std::move(data_header));
    return ports;
}

OutputPorts buildOutputPorts(SharedHeader header)
{
    OutputPorts ports;
    ports.emplace_back(std::move(header));
    return ports;
}

PartitionCursor cursorAt(const Chunk & chunk, size_t row, size_t block_number_pos, size_t block_offset_pos)
{
    const auto & columns = chunk.getColumns();
    return {columns[block_number_pos]->getInt(row), columns[block_offset_pos]->getInt(row)};
}

Field watermarkAt(const Chunk & chunk, size_t row, size_t pos)
{
    Field value;
    chunk.getColumns()[pos]->get(row, value);
    return value;
}

std::shared_ptr<PartitionWatermarkInfo> makePartitionWatermarkInfo(const String & partition_id, const Field & watermark)
{
    auto info = std::make_shared<PartitionWatermarkInfo>();
    info->partition_id = partition_id;
    info->watermark = watermark;
    return info;
}

class AlignStreamsProcessor final : public IProcessor
{
    struct HeldChunk
    {
        Chunk chunk;
        PartitionCursor first;
        PartitionCursor last;
    };

    std::optional<Field> lookupWatermark(const PartitionCursor & target) const
    {
        const auto chunk_it = std::ranges::upper_bound(held_metadata, target, {}, &HeldChunk::first);
        if (chunk_it == held_metadata.begin())
            return std::nullopt;

        const auto & chunk = std::prev(chunk_it)->chunk;
        const auto rows = std::views::iota(size_t{0}, chunk.getNumRows());
        const auto row_it = std::ranges::upper_bound(rows, target, {}, [&](size_t row)
        {
            return cursorAt(chunk, row, metadata_block_number_pos, metadata_block_offset_pos);
        });

        const auto rows_not_above = static_cast<size_t>(std::ranges::distance(rows.begin(), row_it));
        return watermarkAt(chunk, rows_not_above - 1, metadata_watermark_pos);
    }

    bool canReleaseData() const
    {
        return metadata_input.isFinished() || metadata_progress >= held_data->last;
    }

    bool canReleaseMetadata() const
    {
        const auto & front = held_metadata.front();
        if (held_data.has_value())
            return front.last < held_data->last;

        return data_input.isFinished() || data_progress > front.last;
    }

    void releaseData()
    {
        const auto watermark = lookupWatermark(held_data->last);
        if (watermark)
            held_data->chunk.getChunkInfos().add(makePartitionWatermarkInfo(partition_id, *watermark));

        data_progress = held_data->last;
        ready_chunks.push(std::move(held_data->chunk));
        held_data.reset();

        if (watermark && *watermark > last_watermark)
        {
            last_watermark = *watermark;
            ready_chunks.push(WatermarkMarker::create(output.getHeader(), *watermark));
        }
    }

    void releaseMetadata()
    {
        const auto front = std::move(held_metadata.front());
        held_metadata.pop_front();

        const bool overlaps_with_data = held_data.has_value() && held_data->first <= front.last;
        if (overlaps_with_data)
            return;

        const auto watermark = watermarkAt(front.chunk, front.chunk.getNumRows() - 1, metadata_watermark_pos);
        if (watermark <= last_watermark)
            return;

        Chunk info_chunk(output.getHeader().cloneEmptyColumns(), 0);
        info_chunk.getChunkInfos().add(makePartitionWatermarkInfo(partition_id, watermark));
        ready_chunks.push(std::move(info_chunk));

        last_watermark = watermark;
        ready_chunks.push(WatermarkMarker::create(output.getHeader(), watermark));
    }

    bool releaseChunks()
    {
        bool released = false;
        while (true)
        {
            if (!held_metadata.empty() && canReleaseMetadata())
            {
                releaseMetadata();
                released = true;
                continue;
            }

            if (held_data.has_value() && canReleaseData())
            {
                releaseData();
                released = true;
                continue;
            }

            return released;
        }
    }

    bool needMetadata() const
    {
        if (held_metadata.empty())
            return true;

        if (held_data.has_value())
            return metadata_progress < held_data->last;

        return data_input.isFinished();
    }

    bool needData() const
    {
        return !held_data.has_value();
    }

    void consumeMetadataChunk(Chunk chunk)
    {
        const auto cursor_info = chunk.getChunkInfos().getSafe<PartitionCursorInfo>();
        metadata_progress = cursor_info->last;
        held_metadata.push_back(HeldChunk{std::move(chunk), cursor_info->first, cursor_info->last});
    }

    void consumeDataChunk(Chunk chunk)
    {
        const auto cursor_info = chunk.getChunkInfos().getSafe<PartitionCursorInfo>();
        held_data = HeldChunk{std::move(chunk), cursor_info->first, cursor_info->last};
    }

public:
    AlignStreamsProcessor(SharedHeader metadata_header, SharedHeader data_header, String partition_id_, Field initial_watermark_)
        : IProcessor(buildInputPorts(metadata_header, data_header), buildOutputPorts(data_header))
        , partition_id(std::move(partition_id_))
        , metadata_block_number_pos(metadata_header->getPositionByName(BlockNumberColumn::name))
        , metadata_block_offset_pos(metadata_header->getPositionByName(BlockOffsetColumn::name))
        , metadata_watermark_pos(metadata_header->getPositionByName(WatermarkColumn::name))
        , metadata_input(inputs.front())
        , data_input(inputs.back())
        , output(outputs.front())
        , last_watermark(std::move(initial_watermark_))
    {
        if (!last_watermark.isNull())
            ready_chunks.push(WatermarkMarker::create(output.getHeader(), last_watermark));
    }

    String getName() const override { return "AlignStreams"; }

    Status prepare() override
    {
        if (output.isFinished())
        {
            metadata_input.close();
            data_input.close();
            return Status::Finished;
        }

        if (!output.canPush())
            return Status::PortFull;

        while (ready_chunks.empty())
        {
            bool progressed = false;

            if (!metadata_input.isFinished() && needMetadata())
            {
                metadata_input.setNeeded();
                if (metadata_input.hasData())
                {
                    consumeMetadataChunk(metadata_input.pull());
                    progressed = true;
                }
            }

            if (!data_input.isFinished() && needData())
            {
                data_input.setNeeded();
                if (data_input.hasData())
                {
                    consumeDataChunk(data_input.pull());
                    progressed = true;
                }
            }

            if (releaseChunks())
                progressed = true;

            if (!progressed)
                break;
        }

        if (!ready_chunks.empty())
        {
            output.push(std::move(ready_chunks.front()));
            ready_chunks.pop();
            return Status::PortFull;
        }

        if (metadata_input.isFinished() && data_input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        return Status::NeedData;
    }

private:
    const String partition_id;
    const size_t metadata_block_number_pos;
    const size_t metadata_block_offset_pos;
    const size_t metadata_watermark_pos;

    InputPort & metadata_input;
    InputPort & data_input;
    OutputPort & output;

    PartitionCursor metadata_progress;
    std::deque<HeldChunk> held_metadata;

    PartitionCursor data_progress;
    std::optional<HeldChunk> held_data;

    Field last_watermark;
    std::queue<Chunk> ready_chunks;
};

}

AlignStreamsStep::AlignStreamsStep(SharedHeader metadata_header_, SharedHeader data_header_, String partition_id_, Field initial_watermark_)
    : partition_id(std::move(partition_id_))
    , initial_watermark(std::move(initial_watermark_))
{
    updateInputHeaders({std::move(metadata_header_), std::move(data_header_)});
}

void AlignStreamsStep::updateOutputHeader()
{
    output_header = input_headers.back();
}

QueryPipelineBuilderPtr AlignStreamsStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AlignStreams must have two input pipelines");

    if (pipelines[0]->getNumStreams() != 1 || pipelines[1]->getNumStreams() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AlignStreams requires single-stream inputs, got {} and {}", pipelines[0]->getNumStreams(), pipelines[1]->getNumStreams());

    auto processor = std::make_shared<AlignStreamsProcessor>(input_headers.front(), input_headers.back(), partition_id, initial_watermark);
    return QueryPipelineBuilder::mergePipelines(std::move(pipelines[0]), std::move(pipelines[1]), std::move(processor), &processors);
}

void AlignStreamsStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
