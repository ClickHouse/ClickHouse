#include <Storages/MergeTree/Streaming/ReadingPlan/AlignStreams.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <Columns/ColumnsNumber.h>

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/Streaming/Markers.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Core/Block.h>

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

MutableColumnPtr exchangeWithEmpty(MutableColumnPtr & column)
{
    return std::exchange(column, column->cloneEmpty());
}

int compareKeys(UInt64 left_number, UInt64 left_offset, UInt64 right_number, UInt64 right_offset)
{
    if (left_number != right_number)
        return left_number < right_number ? -1 : 1;

    if (left_offset != right_offset)
        return left_offset < right_offset ? -1 : 1;

    return 0;
}

class StreamAligner
{
    void matchPending()
    {
        if (left_columns.empty() || right_columns.empty())
            return;

        const auto & left_block_numbers = assert_cast<const ColumnUInt64 &>(*left_columns[left_block_number_pos]).getData();
        const auto & left_block_offsets = assert_cast<const ColumnUInt64 &>(*left_columns[left_block_offset_pos]).getData();
        const auto & right_block_numbers = assert_cast<const ColumnUInt64 &>(*right_columns[right_block_number_pos]).getData();
        const auto & right_block_offsets = assert_cast<const ColumnUInt64 &>(*right_columns[right_block_offset_pos]).getData();
        const size_t left_rows = left_columns.front()->size();
        const size_t right_rows = right_columns.front()->size();

        const auto compare_at = [&](size_t left, size_t right)
        {
            return compareKeys(left_block_numbers[left], left_block_offsets[left], right_block_numbers[right], right_block_offsets[right]);
        };

        while (processed_left < left_rows && processed_right < right_rows)
        {
            const int comparison = compare_at(processed_left, processed_right);

            if (comparison > 0)
            {
                ++processed_right;
            }
            else if (comparison == 0)
            {
                size_t eq_range_len = 1;
                while (processed_left + eq_range_len < left_rows && processed_right + eq_range_len < right_rows
                    && compare_at(processed_left + eq_range_len, processed_right + eq_range_len) == 0)
                    ++eq_range_len;

                for (size_t i = 0; i < matched_columns.size(); ++i)
                    matched_columns[i]->insertRangeFrom(*right_columns[i], processed_right, eq_range_len);

                for (size_t i = 0; i < attached_positions.size(); ++i)
                    attached_columns[i]->insertRangeFrom(*left_columns[attached_positions[i]], processed_left, eq_range_len);

                processed_right += eq_range_len;
                processed_left += eq_range_len;
            }
            else
            {
                ++processed_left;
            }
        }

        if (processed_left == left_rows)
            dropPendingLeftRows();

        if (processed_right == right_rows)
            dropPendingRightRows();
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
            matched_columns.push_back(type->createColumn());
    }

    bool hasPendingLeftRows() const { return !left_columns.empty(); }
    bool hasPendingRightRows() const { return !right_columns.empty(); }
    bool hasMatchedRows() const { return !matched_columns.front()->empty(); }

    void addRightChunk(Chunk chunk)
    {
        chassert(right_columns.empty());
        if (chunk.getNumRows() == 0)
            return;

        convertToFullIfSparse(chunk);
        convertToFullIfConst(chunk);

        right_columns = chunk.detachColumns();
        processed_right = 0;

        matchPending();
    }

    void addLeftChunk(Chunk chunk)
    {
        chassert(left_columns.empty());
        if (chunk.getNumRows() == 0)
            return;

        convertToFullIfSparse(chunk);
        convertToFullIfConst(chunk);

        left_columns = chunk.detachColumns();
        processed_left = 0;

        matchPending();
    }

    void dropPendingRightRows()
    {
        right_columns.clear();
        processed_right = 0;
    }

    void dropPendingLeftRows()
    {
        left_columns.clear();
        processed_left = 0;
    }

    Chunk flushMatched()
    {
        const size_t rows = matched_columns.front()->size();
        if (rows == 0)
            return {};

        Columns columns;
        columns.reserve(matched_columns.size() + attached_columns.size());

        for (auto & column : matched_columns)
            columns.push_back(exchangeWithEmpty(column));

        for (auto & column : attached_columns)
            columns.push_back(exchangeWithEmpty(column));

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
    size_t processed_left = 0;

    /// Right stream data
    Columns right_columns;
    size_t processed_right = 0;

    /// Matched output
    MutableColumns matched_columns;
    MutableColumns attached_columns;
};

class AlignStreamsProcessor final : public IProcessor
{
    void enqueueChunk(Chunk chunk)
    {
        if (!chunk.empty())
            ready_chunks.push(std::move(chunk));
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

        if (left_input.isFinished())
            enqueueChunk(aligner.flushMatched());

        /// The metadata stream ended - the pending data rows can never be matched.
        if (left_input.isFinished() && !aligner.hasPendingLeftRows() && aligner.hasPendingRightRows())
            aligner.dropPendingRightRows();

        /// The data stream ended - the pending metadata tail can never match.
        if (right_input.isFinished() && !right_chunk.has_value() && !aligner.hasPendingRightRows())
            aligner.dropPendingLeftRows();
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
