#include <Server/DistributedQuery/StreamingExchangeSerializingTransform.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Columns/ColumnString.h>
#include <IO/WriteBufferFromVector.h>

namespace DB
{

StreamingExchangeSerializingTransform::StreamingExchangeSerializingTransform(SharedHeader input_header)
    : ISimpleTransform(std::move(input_header), StreamingExchangeProtocol::packetStreamHeader(), /*skip_empty_chunks_=*/ false)
{
}

void StreamingExchangeSerializingTransform::transform(Chunk & chunk)
{
    /// The packet is written straight into the column, so its bytes are not copied.
    auto column = ColumnString::create();
    auto & chars = column->getChars();
    size_t packet_offset = 0;
    {
        WriteBufferFromVector<ColumnString::Chars> out(chars);
        packet_offset = StreamingExchangeProtocol::writeDataPacket(chunk, getInputPort().getSharedHeader(), out);
        out.finalize();
    }
    StreamingExchangeProtocol::finishDataPacket(reinterpret_cast<char *>(chars.data()) + packet_offset, chars.size() - packet_offset);

    column->getOffsets().push_back(chars.size());

    chunk = Chunk(Columns{std::move(column)}, 1);
}

}
