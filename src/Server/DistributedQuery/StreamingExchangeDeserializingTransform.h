#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Turns exchange packets back into data chunks. The receive steps put one on every pipeline stream
/// behind the sources, so this CPU work runs on all streams in parallel and a source only receives
/// bytes. Each input chunk has one `String` column with one row that holds one packet.
class StreamingExchangeDeserializingTransform final : public ISimpleTransform
{
public:
    StreamingExchangeDeserializingTransform(SharedHeader output_header, String exchange_id_);

    String getName() const override { return "StreamingExchangeDeserializingTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    /// For messages.
    const String exchange_id;
};

}
