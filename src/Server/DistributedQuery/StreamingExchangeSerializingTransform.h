#pragma once

#include <Formats/NativeWriterTypeCache.h>
#include <Processors/ISimpleTransform.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class ICompressionCodec;
using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

/// Serializes and compresses data chunks into exchange packets. The send steps put one on every
/// pipeline stream in front of a sink, so this CPU work runs on all streams in parallel and the
/// sink only sends bytes. Each output chunk has one `String` column with one row that holds one
/// serialized packet.
///
/// A chunk larger than `packet_target_bytes` is sent as several packets, each with a range of its
/// rows, so that a packet, not a whole chunk, is serialized at a time. A packet can be up to twice the
/// target when the sizes of its rows are not known before it is written, and a packet with one row can
/// be up to `max_packet_body_bytes`. A row that does not fit in that throws, or, for an `advisory`
/// stream whose receiver can do without the data (a runtime filter), is not sent. An advisory stream
/// carries at most one row, so a dropped row never leaves a partial state behind.
class StreamingExchangeSerializingTransform final : public ISimpleTransform
{
public:
    StreamingExchangeSerializingTransform(
        SharedHeader input_header,
        CompressionCodecPtr codec_,
        size_t max_packet_body_bytes_ = StreamingExchangeProtocol::DEFAULT_MAX_DATA_PACKET_BODY_BYTES,
        bool advisory_ = false,
        size_t packet_target_bytes_ = StreamingExchangeProtocol::TARGET_DATA_PACKET_BODY_BYTES);

    String getName() const override { return "StreamingExchangeSerializingTransform"; }

protected:
    void transform(Chunk & input_chunk, Chunk & output_chunk) override;
    void transform(Chunk & chunk) override;
    void onFinish() override;
    /// The input chunk is kept until all its rows are sent.
    bool needInputData() const override { return pending_ranges.empty(); }

private:
    struct RowRange
    {
        size_t start = 0;
        size_t length = 0;
    };

    /// Ranges of the same number of rows, by the in-memory size of the chunk.
    static VectorWithMemoryTracking<RowRange> splitEvenly(const Chunk & chunk, size_t target_bytes);
    /// Ranges of `range` by the size of each row, after a packet of `range` turned out too large.
    static VectorWithMemoryTracking<RowRange> splitByRowSizes(const Chunk & chunk, RowRange range, size_t target_bytes);

    /// Writes the next packet of `input_chunk` to `output_chunk`, or leaves it empty for a dropped row.
    void makeNextPacket(Chunk & input_chunk, Chunk & output_chunk);

    /// The packet with the rows of `range`, or nothing when its body would be larger than `max_body_bytes`.
    std::optional<Chunk> trySerialize(const Chunk & chunk, RowRange range, size_t max_body_bytes);

    const CompressionCodecPtr codec;
    const size_t max_packet_body_bytes;
    const bool advisory;
    const size_t packet_target_bytes;
    /// What the writer derived from the column types of this transform, for all its packets.
    NativeWriterTypeCache type_cache;
    /// The row ranges of the current input chunk that are not sent yet, in order.
    DequeWithMemoryTracking<RowRange> pending_ranges;
};

}
