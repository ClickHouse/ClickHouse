#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

class ICompressionCodec;
using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

/// Serializes and compresses data chunks into exchange packets. The send steps put one on every
/// pipeline stream in front of a sink, so this CPU work runs on all streams in parallel and the
/// sink only sends bytes. Each output chunk has one `String` column with one row that holds one
/// serialized packet.
class StreamingExchangeSerializingTransform final : public ISimpleTransform
{
public:
    StreamingExchangeSerializingTransform(SharedHeader input_header, CompressionCodecPtr codec_);

    String getName() const override { return "StreamingExchangeSerializingTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    const CompressionCodecPtr codec;
};

}
