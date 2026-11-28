#include <Server/DistributedQuery/StreamingExchangeSerializingTransform.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <IO/NullWriteBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event RuntimeFilterOversizedStatesRejected;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Writes into `chars` and throws `WriteBuffer::CurrentBufferExhausted` instead of growing it above
/// `max_bytes`, so that an attempt to write a too large packet does not hold the whole packet in memory.
class BoundedCharsWriteBuffer : public WriteBuffer
{
public:
    BoundedCharsWriteBuffer(ColumnString::Chars & chars_, size_t max_bytes_)
        : WriteBuffer(nullptr, 0), chars(chars_), max_bytes(max_bytes_)
    {
        chars.resize_exact(std::min<size_t>(max_bytes, INITIAL_BYTES));
        set(reinterpret_cast<Position>(chars.data()), chars.size());
    }

private:
    /// Small, because the sink keeps a sent packet's capacity until the bytes leave the socket.
    static constexpr size_t INITIAL_BYTES = 256;

    void nextImpl() override
    {
        const size_t written = pos - reinterpret_cast<Position>(chars.data());
        if (written == chars.size())
        {
            if (chars.size() >= max_bytes)
                throw WriteBuffer::CurrentBufferExhausted();
            chars.resize_exact(std::min(max_bytes, 2 * chars.size()));
        }
        internal_buffer = Buffer(reinterpret_cast<Position>(chars.data() + written), reinterpret_cast<Position>(chars.data() + chars.size()));
        working_buffer = internal_buffer;
    }

    void finalizeImpl() override
    {
        chars.resize(pos - reinterpret_cast<Position>(chars.data()));
        bytes += offset();
        set(nullptr, 0);
    }

    ColumnString::Chars & chars;
    const size_t max_bytes;
};

Chunk cutRows(const Chunk & chunk, size_t start, size_t length)
{
    Columns columns;
    columns.reserve(chunk.getNumColumns());
    for (const auto & column : chunk.getColumns())
        columns.push_back(column->cut(start, length));
    Chunk piece(std::move(columns), length);
    piece.setChunkInfos(chunk.getChunkInfos().clone());
    return piece;
}

/// A compressed column cannot be cut. The writer decompresses a whole chunk by itself, so this is needed
/// only before the chunk is cut.
void decompressColumns(Chunk & chunk)
{
    const size_t rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->decompress();
    chunk.setColumns(std::move(columns), rows);
}

}

StreamingExchangeSerializingTransform::StreamingExchangeSerializingTransform(
    SharedHeader input_header, CompressionCodecPtr codec_, size_t max_packet_body_bytes_, bool advisory_, size_t packet_target_bytes_)
    /// Every packet is a chunk with one row; an input chunk whose row is not sent gives no output.
    : ISimpleTransform(std::move(input_header), StreamingExchangeProtocol::packetStreamHeader(), /*skip_empty_chunks_=*/ true)
    , codec(std::move(codec_))
    , max_packet_body_bytes(std::max<size_t>(max_packet_body_bytes_, 1))
    , advisory(advisory_)
    , packet_target_bytes(std::clamp<size_t>(packet_target_bytes_, 1, max_packet_body_bytes))
{
}

void StreamingExchangeSerializingTransform::transform(Chunk &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "StreamingExchangeSerializingTransform makes its packets with the two-argument transform");
}

void StreamingExchangeSerializingTransform::onFinish()
{
    /// The output can close before all packets of a chunk are sent, after a `LIMIT`.
    pending_ranges.clear();
    input_data.chunk.clear();
}

VectorWithMemoryTracking<StreamingExchangeSerializingTransform::RowRange>
StreamingExchangeSerializingTransform::splitEvenly(const Chunk & chunk, size_t target_bytes)
{
    /// The in-memory size, which misses the memory that aggregation states hold outside the column;
    /// a packet that turns out larger is split again in `splitByRowSizes`. A constant is written with
    /// a value for each row.
    const size_t rows = chunk.getNumRows();
    size_t bytes = 0;
    for (const auto & column : chunk.getColumns())
    {
        if (const auto * const_column = typeid_cast<const ColumnConst *>(column.get()))
            bytes += const_column->getDataColumn().byteSize() * rows;
        else
            bytes += column->byteSize();
    }
    const size_t pieces = std::clamp<size_t>((bytes + target_bytes - 1) / target_bytes, 1, std::max<size_t>(rows, 1));

    VectorWithMemoryTracking<RowRange> ranges;
    size_t start = 0;
    for (size_t piece = 0; piece < pieces; ++piece)
    {
        const size_t length = rows / pieces + (piece < rows % pieces ? 1 : 0);
        ranges.push_back({start, length});
        start += length;
    }
    return ranges;
}

VectorWithMemoryTracking<StreamingExchangeSerializingTransform::RowRange>
StreamingExchangeSerializingTransform::splitByRowSizes(const Chunk & chunk, RowRange range, size_t target_bytes)
{
    /// Aggregation states are sized exactly, by serializing each; other columns by their average row.
    const Chunk piece = cutRows(chunk, range.start, range.length);
    VectorWithMemoryTracking<size_t> row_bytes(range.length, 0);
    size_t other_bytes = 0;
    for (const auto & column : piece.getColumns())
    {
        if (const auto * states = typeid_cast<const ColumnAggregateFunction *>(column.get()))
        {
            const auto & function = states->getAggregateFunction();
            for (size_t row = 0; row < range.length; ++row)
            {
                NullWriteBuffer out;
                function->serialize(states->getData()[row], out, /*version=*/ std::nullopt);
                row_bytes[row] += out.count();
            }
        }
        else
        {
            other_bytes += column->byteSize();
        }
    }
    const size_t other_bytes_per_row = other_bytes / range.length;

    VectorWithMemoryTracking<RowRange> ranges;
    size_t start = 0;
    size_t range_bytes = 0;
    for (size_t row = 0; row < range.length; ++row)
    {
        const size_t bytes = other_bytes_per_row + row_bytes[row];
        if (row > start && range_bytes + bytes > target_bytes)
        {
            ranges.push_back({range.start + start, row - start});
            start = row;
            range_bytes = 0;
        }
        range_bytes += bytes;
    }
    ranges.push_back({range.start + start, range.length - start});

    /// The sizes say that the rows fit, but the packet did not: send the halves.
    if (ranges.size() == 1)
    {
        const size_t first_half = range.length / 2;
        ranges = {{range.start, first_half}, {range.start + first_half, range.length - first_half}};
    }
    return ranges;
}

void StreamingExchangeSerializingTransform::transform(Chunk & input_chunk, Chunk & output_chunk)
{
    try
    {
        makeNextPacket(input_chunk, output_chunk);
    }
    catch (...)
    {
        pending_ranges.clear();
        throw;
    }

    /// The input chunk is not needed after its last packet.
    if (pending_ranges.empty())
        input_chunk.clear();
}

void StreamingExchangeSerializingTransform::makeNextPacket(Chunk & input_chunk, Chunk & output_chunk)
{
    if (pending_ranges.empty())
    {
        for (const auto & range : splitEvenly(input_chunk, packet_target_bytes))
            pending_ranges.push_back(range);

        if (pending_ranges.size() > 1)
            decompressColumns(input_chunk);
    }

    while (true)
    {
        const RowRange range = pending_ranges.front();
        pending_ranges.pop_front();

        /// A packet of several rows can be split further, so a too large one is stopped early.
        const size_t max_body_bytes = range.length > 1 ? std::min(2 * packet_target_bytes, max_packet_body_bytes) : max_packet_body_bytes;
        if (auto packet = trySerialize(input_chunk, range, max_body_bytes))
        {
            output_chunk = std::move(*packet);
            break;
        }

        if (range.length > 1)
        {
            /// A compressed column can be much smaller than its packet, so the first split can come only now.
            decompressColumns(input_chunk);
            const auto ranges = splitByRowSizes(input_chunk, range, packet_target_bytes);
            pending_ranges.insert(pending_ranges.begin(), ranges.begin(), ranges.end());
            continue;
        }

        if (!advisory)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "An exchange data packet with {} rows exceeds the maximum body of {} bytes "
                "(setting `distributed_plan_exchange_max_packet_bytes`)",
                range.length, max_packet_body_bytes);

        /// The receiver of an advisory stream (a runtime filter) does without the data that does not arrive.
        ProfileEvents::increment(ProfileEvents::RuntimeFilterOversizedStatesRejected);
        LOG_WARNING(getLogger("StreamingExchangeSerializingTransform"),
            "Not sending a row of an advisory exchange stream: its packet body exceeds the maximum of {} bytes "
            "(setting `distributed_plan_exchange_max_packet_bytes`)",
            max_packet_body_bytes);
        break;
    }
}

std::optional<Chunk> StreamingExchangeSerializingTransform::trySerialize(const Chunk & chunk, RowRange range, size_t max_body_bytes)
{
    /// The packet is written straight into the column, so its bytes are not copied.
    auto column = ColumnString::create();
    auto & chars = column->getChars();
    size_t packet_offset = 0;
    try
    {
        const bool whole_chunk = range.start == 0 && range.length == chunk.getNumRows();
        const Chunk piece = whole_chunk ? Chunk() : cutRows(chunk, range.start, range.length);
        BoundedCharsWriteBuffer out(chars, sizeof(StreamingExchangeProtocol::PacketHeader) + max_body_bytes);
        packet_offset = StreamingExchangeProtocol::writeDataPacket(
            whole_chunk ? chunk : piece, getInputPort().getSharedHeader(), out, codec, &type_cache);
        out.finalize();
    }
    catch (const WriteBuffer::CurrentBufferExhausted &)
    {
        return std::nullopt;
    }

    StreamingExchangeProtocol::finishDataPacket(reinterpret_cast<char *>(chars.data()) + packet_offset, chars.size() - packet_offset);
    column->getOffsets().push_back(chars.size());
    return Chunk(Columns{std::move(column)}, 1);
}

}
