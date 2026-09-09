#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Serializes and compresses data chunks into exchange packets. The send steps put one on every
/// pipeline stream in front of a sink, so this CPU work runs on all streams in parallel and the
/// sink only sends bytes. Each output chunk has one `String` column with one row that holds one
/// serialized packet.
class StreamingExchangeSerializingTransform final : public ISimpleTransform
{
public:
    explicit StreamingExchangeSerializingTransform(SharedHeader input_header);

    String getName() const override { return "StreamingExchangeSerializingTransform"; }

    /// The pipeline header of a stream that carries packets: one `String` column.
    static const SharedHeader & serializedStreamHeader();

    /// Whether a stream with this pipeline header carries packets.
    static bool isSerializedStream(const Block & header);

protected:
    void transform(Chunk & chunk) override;
};

}
