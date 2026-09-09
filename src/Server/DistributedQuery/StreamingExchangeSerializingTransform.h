#pragma once

#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// One ready-to-send exchange data packet. It travels attached to a chunk that has no columns
/// and the row count of the serialized data, and `StreamingExchangeSink` sends the bytes as they are.
struct SerializedExchangePacket : public ChunkInfoCloneable<SerializedExchangePacket>
{
    String bytes;
};

/// Serializes and compresses data chunks into exchange packets. The send steps put one on every
/// pipeline stream in front of a sink, so this CPU work runs on all streams in parallel and the
/// sink only sends bytes.
class StreamingExchangeSerializingTransform final : public ISimpleTransform
{
public:
    explicit StreamingExchangeSerializingTransform(SharedHeader input_header);

    String getName() const override { return "StreamingExchangeSerializingTransform"; }

protected:
    void transform(Chunk & chunk) override;
};

}
