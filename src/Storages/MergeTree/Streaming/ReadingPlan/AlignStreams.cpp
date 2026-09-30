#include <Storages/MergeTree/Streaming/ReadingPlan/AlignStreams.h>
#include <Storages/MergeTree/Streaming/Cursors/CursorUtils.h>
#include <Storages/MergeTree/Streaming/ReadingPlan/StampPartitionCursors.h>
#include <Storages/MergeTree/Streaming/ReadingPlan/StampPartitionWatermarks.h>

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/Streaming/Markers.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Core/Block.h>

#include <Common/Exception.h>

#include <optional>
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

bool isWatermarkChunk(const Chunk & chunk)
{
    return chunk.getChunkInfos().has<WatermarkMarker>()
        || chunk.getChunkInfos().has<PartitionWatermarkInfo>();
}

class AlignStreamsProcessor final : public IProcessor
{
    struct HeldWatermark
    {
        Chunk chunk;
        PartitionCursor boundary;
    };

    struct HeldData
    {
        Chunk chunk;
        PartitionCursor first;
        PartitionCursor last;
    };

    bool canReleaseWatermark(const HeldWatermark & watermark) const
    {
        if (data_input.isFinished() && !held_data.has_value())
            return true;

        if (held_data.has_value())
            return held_data->first > watermark.boundary;

        return data_progress > watermark.boundary;
    }

    bool canReleaseData(const HeldData & data) const
    {
        if (metadata_input.isFinished())
            return true;

        return !held_watermarks.empty() && held_watermarks.back().boundary >= data.last;
    }

    void releaseData()
    {
        data_progress = held_data->last;
        ready_chunks.push(std::move(held_data->chunk));
        held_data.reset();
    }

    void releaseWatermark()
    {
        ready_chunks.push(std::move(held_watermarks.front().chunk));
        held_watermarks.pop();
    }

    void releaseChunks()
    {
        while (true)
        {
            if (!held_watermarks.empty() && canReleaseWatermark(held_watermarks.front()))
            {
                releaseWatermark();
                continue;
            }

            if (held_data.has_value() && canReleaseData(*held_data))
            {
                releaseData();
                continue;
            }

            break;
        }
    }

    void consumeMetadataChunk(Chunk chunk)
    {
        if (auto info = chunk.getChunkInfos().extract<PartitionCursorInfo>())
            metadata_progress = info->last;

        if (isWatermarkChunk(chunk))
        {
            Chunk watermark_chunk(output.getHeader().cloneEmptyColumns(), 0);
            watermark_chunk.setChunkInfos(std::move(chunk.getChunkInfos()));
            held_watermarks.push(HeldWatermark{std::move(watermark_chunk), metadata_progress});
        }
    }

    void consumeDataChunk(Chunk chunk)
    {
        if (auto info = chunk.getChunkInfos().get<PartitionCursorInfo>())
           held_data = HeldData{std::move(chunk), info->first, info->last};
    }

public:
    AlignStreamsProcessor(SharedHeader metadata_header, SharedHeader data_header)
        : IProcessor(buildInputPorts(metadata_header, data_header), buildOutputPorts(data_header))
        , metadata_input(inputs.front())
        , data_input(inputs.back())
        , output(outputs.front())
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

        if (ready_chunks.empty())
        {
            if (!metadata_input.isFinished())
            {
                metadata_input.setNeeded();
                if (metadata_input.hasData())
                    consumeMetadataChunk(metadata_input.pull());
            }

            if (!held_data.has_value() && !data_input.isFinished())
            {
                data_input.setNeeded();
                if (data_input.hasData())
                    consumeDataChunk(data_input.pull());
            }

            releaseChunks();
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
    InputPort & metadata_input;
    InputPort & data_input;
    OutputPort & output;

    PartitionCursor metadata_progress;
    std::queue<HeldWatermark> held_watermarks;

    PartitionCursor data_progress;
    std::optional<HeldData> held_data;

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AlignStreams must have two input pipelines");

    if (pipelines[0]->getNumStreams() != 1 || pipelines[1]->getNumStreams() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AlignStreams requires single-stream inputs, got {} and {}", pipelines[0]->getNumStreams(), pipelines[1]->getNumStreams());

    auto processor = std::make_shared<AlignStreamsProcessor>(input_headers.front(), input_headers.back());
    return QueryPipelineBuilder::mergePipelines(std::move(pipelines[0]), std::move(pipelines[1]), std::move(processor), &processors);
}

void AlignStreamsStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
