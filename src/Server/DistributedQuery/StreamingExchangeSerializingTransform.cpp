#include <Server/DistributedQuery/StreamingExchangeSerializingTransform.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

StreamingExchangeSerializingTransform::StreamingExchangeSerializingTransform(SharedHeader input_header)
    : ISimpleTransform(std::move(input_header), std::make_shared<const Block>(), /*skip_empty_chunks_=*/ false)
{
}

void StreamingExchangeSerializingTransform::transform(Chunk & chunk)
{
    WriteBufferFromOwnString buffer;
    StreamingExchangeProtocol::writeDataPacket(chunk, getInputPort().getSharedHeader(), buffer);

    auto packet = std::make_shared<SerializedExchangePacket>();
    packet->bytes = std::make_shared<const String>(std::move(buffer.str()));

    /// The new chunk carries only the packet; the aggregation info, if any, is inside it.
    Chunk result(Columns{}, chunk.getNumRows());
    result.getChunkInfos().add(std::move(packet));
    chunk = std::move(result);
}

}
