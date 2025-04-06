#include <Server/DistributedQuery/StreamingExchangeSource.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeReader.h>
#include <Core/ProtocolDefines.h>
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

    if (finished_reading)
    {
        output.finish();
        return Status::Finished;
    }

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

    if (in.available() > 0)
        return Status::Ready;

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
    readVarUInt(packet_type, in);
    if (packet_type != StreamingExchangeProtocol::PacketType::Data)
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
        rows_read += num_rows;

        LOG_TEST(log, "Received chunk with {} rows from exchange stream {}", num_rows, stream_name);
    }
    else
    {
        /// Empty chunk means end of stream.
        finished_reading = true;
        LOG_TRACE(log, "Finished reading from exchange stream {}, total rows: {}, bytes: {}",
            stream_name, rows_read, in.count());
    }

    return result;
}

}
