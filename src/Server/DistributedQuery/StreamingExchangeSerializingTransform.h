#pragma once

#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// One ready-to-send exchange data packet, attached to a chunk that has no columns and the row
/// count of the serialized data. `StreamingExchangeSink` sends the bytes as they are. The bytes are
/// shared because a broadcast copies the chunk once per destination.
struct SerializedExchangePacket : public ChunkInfoCloneable<SerializedExchangePacket>
{
    std::shared_ptr<const String> bytes;
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
