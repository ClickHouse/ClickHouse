#include <Processors/Sinks/NativeCompressedSink.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

namespace DB
{

NativeCompressedSink::~NativeCompressedSink()
{
    writer.reset();

    if (compressed_buf && !compressed_buf->isFinalized())
        compressed_buf->cancel();

    if (!out.isFinalized())
        out.cancel();
}

void NativeCompressedSink::initWriterOnce(const Chunk & chunk)
{
    if (input.getHeader().empty()) /// No input columns? (case of `SELECT count()`)
        return;

    if (!writer)
    {
        const bool has_aggregated_chunk_info = !!chunk.getChunkInfos().get<AggregatedChunkInfo>();
        UInt64 stream_flags = 0;
        if (has_aggregated_chunk_info)
            stream_flags |= 1;
        writeVarUInt(stream_flags, out);

        compressed_buf = std::make_unique<CompressedWriteBuffer>(out);
        writer = std::make_unique<NativeWriter>(*compressed_buf, DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS, input.getSharedHeader());
    }
}

void NativeCompressedSink::consume(Chunk chunk)
{
    rows_written += chunk.getNumRows();

    if (input.getHeader().empty()) /// Blocks without columns will not be written, we will write total rows count only at the end.
        return;

    initWriterOnce(chunk);

    LOG_TEST(log, "Writing chunk with {} rows to stream {}", chunk.getNumRows(), stream_name);

    Block block = input.getHeader().cloneWithColumns(chunk.getColumns());
    writer->write(block);
}

void NativeCompressedSink::onFinish()
{
    if (input.getHeader().empty())
    {
        /// Only write total rows count.
        writeVarUInt(rows_written, out);
    }
    else
    {
        initWriterOnce(Chunk{});    /// In case now chunks were written
        writer->flush();
        compressed_buf->finalize();
    }
    out.finalize();

    LOG_TEST(log, "Finished writing to stream {}, total rows: {}, bytes: {}", stream_name, rows_written, out.count());
}

}
