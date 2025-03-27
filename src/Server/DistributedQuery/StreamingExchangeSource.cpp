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

IProcessor::Status StreamingExchangeSource::prepare()
{
    LOG_TEST(log, "Prepare exchange stream {}", stream_name);

    /// Check can output.
    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (has_input)
    {
        output.pushData(std::move(current_chunk));
        has_input = false;
        return Status::PortFull;
    }

    /// TODO: handle cancelled state?

    if (finished_reading)
    {
        getPort().finish();
        return Status::Finished;
    }
    return Status::Async;
}

int StreamingExchangeSource::schedule()
{
    LOG_TEST(log, "Schedule exchange stream {}, fd: {}", stream_name, socket.sockfd());

    return socket.sockfd();
}

Chunk StreamingExchangeSource::generate()
{
    LOG_TEST(log, "Reading from exchange stream {}", stream_name);

    UInt64 packet_type = 0;
    readVarUInt(packet_type, in); /// TODO: use separate protocol for streaming exchange
    if (packet_type != Protocol::Client::Data)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet type {}", packet_type);

    UInt64 num_rows = 0;
    readVarUInt(num_rows, in);

    Chunk result;
    if (num_rows != 0)
    {
        auto compressed_buf = std::make_unique<CompressedReadBuffer>(in);
        auto reader = std::make_unique<NativeReader>(*compressed_buf, output.getHeader(), DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS);
        Block block = reader->read();
        result = Chunk(block.getColumns(), num_rows);
    }
    else
    {
        finished_reading = true;
    }

    LOG_TEST(log, "Received chunk with {} rows from exchange stream {}", num_rows, stream_name);

    return result;
}

}
