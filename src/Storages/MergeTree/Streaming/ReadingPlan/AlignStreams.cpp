#include <Storages/MergeTree/Streaming/ReadingPlan/AlignStreams.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <Columns/ColumnsNumber.h>

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/Streaming/Markers.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Core/Block.h>
#include <Core/Defines.h>

#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <base/defines.h>

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

InputPorts buildInputPorts(SharedHeader left_header, SharedHeader right_header)
{
    InputPorts ports;
    ports.emplace_back(std::move(left_header));
    ports.emplace_back(std::move(right_header));
    return ports;
}

OutputPorts buildOutputPorts(SharedHeader header)
{
    OutputPorts ports;
    ports.emplace_back(std::move(header));
    return ports;
}

Block buildOutputHeader(const Block & left_header, const Block & right_header)
{
    Block header = right_header;
    for (const auto & column : left_header)
        if (!header.has(column.name))
            header.insert(column);

    return header;
}

std::vector<size_t> collectAttachedPositions(const Block & left_header, const Block & right_header)
{
    std::vector<size_t> positions;
    for (size_t position = 0; position < left_header.columns(); ++position)
        if (!right_header.has(left_header.getByPosition(position).name))
            positions.push_back(position);

    return positions;
}

const ColumnUInt64 * keyColumn(const IColumn & column)
{
    return assert_cast<const ColumnUInt64 *>(&column);
}

MutableColumnPtr exchangeWithEmpty(MutableColumnPtr & column)
{
    return std::exchange(column, column->cloneEmpty());
}

class StreamAligner
{
    void matchPending()
    {
        if (left_columns.empty())
            return;

        const auto * left_block_numbers = keyColumn(*left_columns[left_block_number_pos]);
        const auto * left_block_offsets = keyColumn(*left_columns[left_block_offset_pos]);
        const auto * right_block_numbers = keyColumn(*data_columns[right_block_number_pos]);
        const auto * right_block_offsets = keyColumn(*data_columns[right_block_offset_pos]);
        const size_t left_rows = left_columns.front()->size();

        while (left_row < left_rows && hasPendingRightRows())
        {
            const std::pair left_key{left_block_numbers->getElement(left_row), left_block_offsets->getElement(left_row)};
            const std::pair right_key{right_block_numbers->getElement(matched), right_block_offsets->getElement(matched)};

            if (left_key > right_key)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "A data stream row of AlignStreams has no matching metadata stream row (block number {}, block offset {})",
                    right_key.first, right_key.second);
            }
            else if (left_key == right_key)
            {
                for (size_t i = 0; i < attached_positions.size(); ++i)
                    attached_columns[i]->insertFrom(*left_columns[attached_positions[i]], left_row);

                ++matched;
            }
            else
            {
                ++left_row;
            }
        }

        if (left_row == left_rows)
            dropPendingLeftRows();
    }

public:
    StreamAligner(const Block & left_header, const Block & right_header)
        : right_block_number_pos(right_header.getPositionByName(BlockNumberColumn::name))
        , right_block_offset_pos(right_header.getPositionByName(BlockOffsetColumn::name))
        , left_block_number_pos(left_header.getPositionByName(BlockNumberColumn::name))
        , left_block_offset_pos(left_header.getPositionByName(BlockOffsetColumn::name))
        , attached_positions(collectAttachedPositions(left_header, right_header))
    {
        for (const auto position : attached_positions)
            attached_columns.push_back(left_header.getByPosition(position).type->createColumn());

        for (const auto & type : right_header.getDataTypes())
            data_columns.push_back(type->createColumn());
    }

    bool hasPendingLeftRows() const { return !left_columns.empty(); }
    bool hasPendingRightRows() const { return matched < data_columns.front()->size(); }
    bool hasMatchedRows() const { return matched > 0; }

    void addRightChunk(Chunk chunk)
    {
        const size_t rows = chunk.getNumRows();
        if (rows == 0)
            return;

        /// The rows are copied with insertRangeFrom, which requires plain full source columns.
        convertToFullIfSparse(chunk);
        convertToFullIfConst(chunk);

        const auto & source_columns = chunk.getColumns();
        for (size_t i = 0; i < data_columns.size(); ++i)
            data_columns[i]->insertRangeFrom(*source_columns[i], 0, rows);

        matchPending();
    }

    void addLeftChunk(Chunk chunk)
    {
        chassert(left_columns.empty());
        if (chunk.getNumRows() == 0)
            return;

        /// The rows are read with getElement/insertFrom, which require plain full source columns.
        convertToFullIfSparse(chunk);
        convertToFullIfConst(chunk);

        left_columns = chunk.detachColumns();
        left_row = 0;

        matchPending();
    }

    void dropPendingLeftRows()
    {
        left_columns.clear();
        left_row = 0;
    }

    std::optional<Chunk> flushMatched(size_t min_rows = 0)
    {
        if (matched == 0 || matched < min_rows)
            return std::nullopt;

        const size_t total = data_columns.front()->size();
        const size_t rows = std::exchange(matched, 0);

        Columns columns;
        columns.reserve(data_columns.size() + attached_columns.size());

        for (auto & column : data_columns)
        {
            columns.push_back(column->cut(0, rows));
            column = IColumn::mutate(column->cut(rows, total - rows));
        }

        for (auto & column : attached_columns)
        {
            chassert(column->size() == rows);
            columns.push_back(exchangeWithEmpty(column));
        }

        return Chunk(std::move(columns), rows);
    }

private:
    const size_t right_block_number_pos;
    const size_t right_block_offset_pos;
    const size_t left_block_number_pos;
    const size_t left_block_offset_pos;
    const std::vector<size_t> attached_positions;

    /// Left stream data
    Columns left_columns;
    size_t left_row = 0;

    /// Right stream data
    MutableColumns data_columns;
    MutableColumns attached_columns;
    size_t matched = 0;
};

class AlignStreamsProcessor final : public IProcessor
{
    void enqueueChunk(std::optional<Chunk> chunk)
    {
        if (chunk.has_value())
            ready_chunks.push(std::move(*chunk));
    }

    void enqueueInfos(Chunk::ChunkInfoCollection && infos)
    {
        Chunk info_chunk(output.getHeader().cloneEmptyColumns(), 0);
        info_chunk.setChunkInfos(std::move(infos));
        enqueueChunk(std::move(info_chunk));
    }

public:
    AlignStreamsProcessor(SharedHeader left_header, SharedHeader right_header)
        : IProcessor(
              buildInputPorts(left_header, right_header),
              buildOutputPorts(std::make_shared<const Block>(buildOutputHeader(*left_header, *right_header))))
        , left_input(inputs.front())
        , right_input(inputs.back())
        , output(outputs.front())
        , aligner(*left_header, *right_header)
    {
    }

    String getName() const override { return "AlignStreams"; }

    Status prepare() override
    {
        if (output.isFinished())
        {
            left_input.close();
            right_input.close();
            return Status::Finished;
        }

        if (!output.canPush())
        {
            left_input.setNotNeeded();
            right_input.setNotNeeded();
            return Status::PortFull;
        }

        if (!ready_chunks.empty())
        {
            output.push(std::move(ready_chunks.front()));
            ready_chunks.pop();
            return Status::PortFull;
        }

        if (!left_chunk.has_value() && !aligner.hasPendingLeftRows() && !left_input.isFinished())
        {
            left_input.setNeeded();
            if (!left_input.hasData())
                return Status::NeedData;

            left_chunk = left_input.pull(/*set_not_needed=*/true);
        }

        /// An empty metadata chunk (infos) is processable without data-side progress.
        const bool left_infos_only = left_chunk.has_value() && left_chunk->getNumRows() == 0;

        if (!left_infos_only && !right_chunk.has_value() && !aligner.hasPendingRightRows() && !right_input.isFinished())
        {
            right_input.setNeeded();
            if (!right_input.hasData())
                return Status::NeedData;

            right_chunk = right_input.pull(/*set_not_needed=*/true);
        }

        const bool has_work = left_chunk.has_value() || right_chunk.has_value() || aligner.hasPendingLeftRows() || aligner.hasPendingRightRows() || aligner.hasMatchedRows();

        if (!has_work)
        {
            chassert(left_input.isFinished() && right_input.isFinished());
            output.finish();
            return Status::Finished;
        }

        return Status::Ready;
    }

    void work() override
    {
        if (right_chunk.has_value())
        {
            aligner.addRightChunk(std::move(*right_chunk));
            right_chunk.reset();
        }

        if (left_chunk.has_value())
        {
            if (isMarkerChunk(*left_chunk))
            {
                enqueueChunk(aligner.flushMatched());
                enqueueInfos(std::move(left_chunk->getChunkInfos()));
            }
            else
            {
                aligner.addLeftChunk(std::move(*left_chunk));
            }

            left_chunk.reset();
        }

        /// Data rows left without metadata coverage - invariant is broken.
        if (left_input.isFinished() && !aligner.hasPendingLeftRows() && aligner.hasPendingRightRows())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The metadata stream of AlignStreams ended before all data stream rows were matched");

        /// The data stream ended - the pending metadata tail can never match.
        if (right_input.isFinished() && !right_chunk.has_value() && !aligner.hasPendingRightRows())
            aligner.dropPendingLeftRows();

        const bool streams_done = left_input.isFinished() && right_input.isFinished();
        enqueueChunk(aligner.flushMatched(streams_done ? 0 : DEFAULT_BLOCK_SIZE));
    }

private:
    InputPort & left_input;
    InputPort & right_input;
    OutputPort & output;

    std::optional<Chunk> left_chunk;
    std::optional<Chunk> right_chunk;

    StreamAligner aligner;
    std::queue<Chunk> ready_chunks;
};

}

AlignStreamsStep::AlignStreamsStep(SharedHeader left_header_, SharedHeader right_header_)
{
    updateInputHeaders({std::move(left_header_), std::move(right_header_)});
}

void AlignStreamsStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(buildOutputHeader(*input_headers.front(), *input_headers.back()));
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
