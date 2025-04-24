#include <Server/DistributedQuery/StreamingExchangeSource.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Processors/Transforms/AggregatingTransform.h>
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

    if (in.available() > 0)
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

Chunk StreamingExchangeSource::generate()
{
    if (output_finished)
    {
        LOG_TRACE(log, "NoMoreDataNeeded from exchange stream {}, total rows: {}, bytes: {}", stream_name, rows_read, in.count());

        sendNoMoreDataNeeded();
        finished_reading = true;
        return {};
    }

    LOG_TEST(log, "Reading from exchange stream {}", stream_name);

    UInt64 packet_type = 0;
    readVarUInt(packet_type, in);
    if (packet_type != StreamingExchangeProtocol::PacketType::Data)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet type {}", packet_type);

    UInt64 num_rows = 0;
    readVarUInt(num_rows, in);
    UInt64 num_columns = 0;
    readVarUInt(num_columns, in);

    Chunk result;
    if (num_rows != 0)
    {
        auto compressed_buf = std::make_unique<CompressedReadBuffer>(in);
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

        LOG_TEST(log, "Received chunk with {} rows from exchange stream {}", num_rows, stream_name);
    }
    else if (num_columns != 0)
    {
        /// Empty chunk with non-zero number of columns
        LOG_TEST(log, "Received empty chunk with {} columns from exchange stream {}", num_columns, stream_name);
        result = Chunk(output.getHeader().cloneEmptyColumns(), 0);
    }
    else
    {
        /// Empty chunk with no columns means end of stream.
        finished_reading = true;
        LOG_TRACE(log, "Finished reading from exchange stream {}, total rows: {}, bytes: {}",
            stream_name, rows_read, in.count());
    }

    return result;
}

}
