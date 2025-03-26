#include <Server/DistributedQuery/StreamingExchangeSource.h>
#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeReader.h>
#include <Core/ProtocolDefines.h>
#include <Core/Protocol.h>
#include <IO/ReadHelpers.h>
#include <Common/logger_useful.h>
#include "base/types.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

Chunk StreamingExchangeSource::generate()
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, in); /// TODO: use separate protocol for streaming exchange
    if (packet_type != Protocol::Client::Data)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet type {}", packet_type);

    UInt64 num_rows = 0;
    readVarUInt(num_rows, in);

    LOG_TEST(log, "Reading chunk with {} rows from exchange stream {}", num_rows, stream_name);

    if (num_rows == 0)
        return {};

    auto compressed_buf = std::make_unique<CompressedReadBuffer>(in);
    auto reader = std::make_unique<NativeReader>(*compressed_buf, output.getHeader(), DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS);

    Block block = reader->read();

    return Chunk(block.getColumns(), num_rows);
}

}
