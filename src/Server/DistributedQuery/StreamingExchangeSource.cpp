#include <memory>
#include <Server/DistributedQuery/StreamingExchangeSource.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeReader.h>
#include <Core/ProtocolDefines.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/NetException.h>
#include <Common/logger_useful.h>
#include "base/types.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

StreamingExchangeSource::~StreamingExchangeSource()
{
    if (!out.isFinalized())
        out.cancel();
}

IProcessor::Status StreamingExchangeSource::prepare()
{
    LOG_TEST(log, "Prepare exchange source {}", stream_name);

    if (finished_reading)
    {
        output.finish();
        return Status::Finished;
    }

    /// Check can output.
    if (output.isFinished())
    {
        output_finished = true;
        /// Return Status::Ready because we still need to send NoMoreDataNeeded packet to sink before closing the socket to let it know that this is not a disconnect.
        return Status::Ready;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (has_input)
    {
        output.pushData(std::move(current_chunk));
        has_input = false;
        return Status::PortFull;
    }

    /// TODO: handle cancelled state?

    if (packet_in)
        return Status::Ready;

    return Status::Async;
}

int StreamingExchangeSource::schedule()
{
    LOG_TEST(log, "Schedule exchange stream {}, fd: {}", stream_name, socket.sockfd());

    return socket.sockfd();
}

void StreamingExchangeSource::sendNoMoreDataNeeded()
{
    writeVarUInt(StreamingExchangeProtocol::PacketType::NoMoreDataNeeded, out);
    out.next();
}

void StreamingExchangeSource::readFromSocket(char * buffer, size_t buffer_size, size_t & position)
{
    while (position < buffer_size)
    {
        size_t remaining_size = buffer_size - position;

        ssize_t received = socket.receiveBytes(buffer + position, remaining_size);
        if (received < 0)
        {
            auto last_error = errno;
            if (last_error == EINTR)
            {
                continue;
            }
            else if (last_error == EAGAIN || last_error == EWOULDBLOCK)
            {
                /// Socket is not ready for reading, wait for epoll event
                break;
            }
            else
            {
                throw Poco::Net::NetException("Failed to receive data from socket for exchange {}, error {}", stream_name, last_error);
            }
        }
        else if (received == 0)
        {
            throw Poco::Net::NetException("Failed to receive data from socket for exchange {}, socket was unexpectedly closed", stream_name);
        }

        LOG_TEST(log, "Received {} bytes from exchange stream {}, fd: {}", received, stream_name, socket.sockfd());

        position += received;
        bytes_read += received;
    }
}

void StreamingExchangeSource::tryReadHeader()
{
    /// Read remaining size to header buffer
    readFromSocket(reinterpret_cast<char*>(&current_packet_header) , sizeof(current_packet_header), current_packet_header_bytes_filled);
    if (current_packet_header_bytes_filled == sizeof(current_packet_header))
    {
        if (current_packet_header.packet_type != StreamingExchangeProtocol::PacketType::Data)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet type {}", current_packet_header.packet_type);

        current_packet_body.resize(current_packet_header.bytes_size);
        current_packet_body_bytes_filled = 0;
        packet_receive_state = ReceivingBody;

        LOG_TEST(log, "Expecting packet with {} bytes from exchange stream {}, fd: {}", current_packet_header.bytes_size, stream_name, socket.sockfd());
    }
}

void StreamingExchangeSource::tryReadBody()
{
    /// Read remaining size of the packet
    readFromSocket(current_packet_body.data() , current_packet_body.size(), current_packet_body_bytes_filled);
    if (current_packet_body_bytes_filled == current_packet_body.size())
    {
        packet_receive_state = ReceivingHeader;
        current_packet_header_bytes_filled = 0;
        packet_in = std::make_unique<ReadBufferFromMemory>(current_packet_body.data(), current_packet_body.size());
    }
}

std::optional<Chunk> StreamingExchangeSource::tryGenerate()
{
    if (output_finished)
    {
        LOG_TRACE(log, "NoMoreDataNeeded from exchange stream {}, total rows: {}, bytes: {}", stream_name, rows_read, bytes_read);

        sendNoMoreDataNeeded();
        finished_reading = true;
        return {};
    }

    LOG_TEST(log, "Reading from exchange stream {}", stream_name);

    if (packet_receive_state == ReceivingHeader)
        tryReadHeader();

    if (packet_receive_state == ReceivingBody)
        tryReadBody();

    /// If a whole packet has been read, we can parse it.
    if (!packet_in)
        return Chunk(); /// Empty chunk means that we currently heve no data but we have not finished yet.

    UInt64 num_rows = 0;
    readVarUInt(num_rows, *packet_in);
    UInt64 num_columns = 0;
    readVarUInt(num_columns, *packet_in);

    std::optional<Chunk> result;
    if (num_columns != 0)
    {
        auto compressed_buf = std::make_unique<CompressedReadBuffer>(*packet_in);
        auto reader = std::make_unique<NativeReader>(*compressed_buf, output.getHeader(), DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS);
        Block block = reader->read();

        result = Chunk(block.getColumns(), num_rows);
        /// TODO: is this enough for passing chunk infos?
        {
            auto info = std::make_shared<AggregatedChunkInfo>();
            info->bucket_num = block.info.bucket_num;
            info->is_overflows = block.info.is_overflows;
            result->getChunkInfos().add(std::move(info));
        }
        rows_read += num_rows;

        LOG_TEST(log, "Received chunk with {} rows and {} columns from exchange stream {}", num_rows, num_columns, stream_name);
    }
    else if (num_rows == 0)
    {
        LOG_TEST(log, "Received empty chunk from exchange stream {}", stream_name);
        result = Chunk(output.getHeader().cloneEmptyColumns(), 0);
    }
    else
    {
        LOG_TEST(log, "Received chunk with {} rows and no columns from exchange stream {}", num_rows, stream_name);
        result = Chunk(Columns{}, num_rows);
    }

    if (num_rows == 0 && num_columns == 0)
    {
        /// Empty chunk with no columns means end of stream.
        finished_reading = true;
        LOG_TRACE(log, "Finished reading from exchange stream {}, total rows: {}, bytes: {}",
            stream_name, rows_read, bytes_read);
    }

    packet_in.reset();
    packet_receive_state = ReceivingHeader;
    current_packet_body.clear();
    current_packet_body_bytes_filled = 0;

    return result;
}

}
