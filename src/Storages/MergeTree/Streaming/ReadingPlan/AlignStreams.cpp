#include <Storages/MergeTree/Streaming/ReadingPlan/AlignStreams.h>
#include <Storages/MergeTree/Streaming/Cursors/CursorUtils.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <Columns/IColumn.h>

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Core/Block.h>

#include <Common/Exception.h>

#include <queue>
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

PartitionCursor chunkLastRowCursor(const Chunk & chunk, size_t block_number_pos, size_t block_offset_pos)
{
    const auto & columns = chunk.getColumns();
    const size_t last_row = chunk.getNumRows() - 1;
    return {columns[block_number_pos]->getInt(last_row), columns[block_offset_pos]->getInt(last_row)};
}

class AlignStreamsProcessor final : public IProcessor
{
    struct HeldMarker
    {
        Chunk::ChunkInfoCollection infos;
        PartitionCursor boundary;
    };

    bool isReleasable(const HeldMarker & marker) const
    {
        if (data_input.isFinished())
            return true;

        return data_progress > marker.boundary;
    }

    void releaseMarkers()
    {
        while (!held_markers.empty() && isReleasable(held_markers.front()))
        {
            Chunk marker_chunk(output.getHeader().cloneEmptyColumns(), 0);
            marker_chunk.setChunkInfos(std::move(held_markers.front().infos));
            ready_chunks.push(std::move(marker_chunk));
            held_markers.pop();
        }
    }

    void consumeMetadataChunk(Chunk chunk)
    {
        if (chunk.getNumRows() > 0)
            metadata_progress = chunkLastRowCursor(chunk, metadata_block_number_pos, metadata_block_offset_pos);

        if (!chunk.getChunkInfos().empty())
            held_markers.push({std::move(chunk.getChunkInfos()), metadata_progress});
    }

    void consumeDataChunk(Chunk chunk)
    {
        if (chunk.getNumRows() == 0)
            return;

        data_progress = chunkLastRowCursor(chunk, data_block_number_pos, data_block_offset_pos);
        ready_chunks.push(std::move(chunk));
    }

public:
    AlignStreamsProcessor(SharedHeader metadata_header, SharedHeader data_header)
        : IProcessor(buildInputPorts(metadata_header, data_header), buildOutputPorts(data_header))
        , metadata_input(inputs.front())
        , data_input(inputs.back())
        , output(outputs.front())
        , metadata_block_number_pos(metadata_header->getPositionByName(BlockNumberColumn::name))
        , metadata_block_offset_pos(metadata_header->getPositionByName(BlockOffsetColumn::name))
        , data_block_number_pos(data_header->getPositionByName(BlockNumberColumn::name))
        , data_block_offset_pos(data_header->getPositionByName(BlockOffsetColumn::name))
    {
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

        if (!metadata_input.isFinished())
        {
            metadata_input.setNeeded();
            if (metadata_input.hasData())
                consumeMetadataChunk(metadata_input.pull());
        }

        if (!data_input.isFinished())
        {
            data_input.setNeeded();
            if (data_input.hasData())
                consumeDataChunk(data_input.pull());
        }

        releaseMarkers();

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
    InputPort & metadata_input;
    InputPort & data_input;
    OutputPort & output;

    const size_t metadata_block_number_pos;
    const size_t metadata_block_offset_pos;
    const size_t data_block_number_pos;
    const size_t data_block_offset_pos;

    PartitionCursor metadata_progress;
    std::queue<HeldMarker> held_markers;

    PartitionCursor data_progress;
    std::queue<Chunk> ready_chunks;
};

}

AlignStreamsStep::AlignStreamsStep(SharedHeader metadata_header_, SharedHeader data_header_)
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AlignStreamsStep must have two input pipelines");

    if (pipelines[0]->getNumStreams() != 1 || pipelines[1]->getNumStreams() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AlignStreamsStep requires single-stream inputs, got {} and {}", pipelines[0]->getNumStreams(), pipelines[1]->getNumStreams());

    auto processor = std::make_shared<AlignStreamsProcessor>(input_headers.front(), input_headers.back());
    return QueryPipelineBuilder::mergePipelines(std::move(pipelines[0]), std::move(pipelines[1]), std::move(processor), &processors);
}

void AlignStreamsStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
