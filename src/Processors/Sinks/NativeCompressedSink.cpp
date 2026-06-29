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

void NativeCompressedSink::initWriterOnce()
{
    if (input.getHeader().empty()) /// No input columns? (case of `SELECT count()`)
        return;

    if (!writer)
    {
        compressed_buf = std::make_unique<CompressedWriteBuffer>(out);
        writer = std::make_unique<NativeWriter>(*compressed_buf, DBMS_TCP_PROTOCOL_VERSION, input.getSharedHeader());
    }
}

void NativeCompressedSink::consume(Chunk chunk)
{
    rows_written += chunk.getNumRows();

    if (input.getHeader().empty()) /// Blocks without columns will not be written, we will write total rows count only at the end.
        return;

    initWriterOnce();

    LOG_TEST(log, "Writing chunk with {} rows to stream {}", chunk.getNumRows(), stream_name);

    Block block = input.getHeader().cloneWithColumns(chunk.getColumns());
    auto agg_info = chunk.getChunkInfos().get<AggregatedChunkInfo>();

    /// Prefix each block with a per-block flag so a stream may freely mix blocks with and without
    /// aggregation metadata. A stream-level flag would lose or fabricate metadata on a mixed stream.
    UInt64 block_flags = 0;
    if (agg_info)
        block_flags |= 1;
    writeVarUInt(block_flags, *compressed_buf);

    if (agg_info)
    {
        /// Carry most aggregation metadata in block.info so the reader can reconstruct AggregatedChunkInfo.
        block.info.bucket_num = agg_info->bucket_num;
        block.info.is_overflows = agg_info->is_overflows;
        block.info.out_of_order_buckets = agg_info->out_of_order_buckets;
        /// chunk_num has no BlockInfo field; write it next to the block so memory-bound merging can restore order.
        writeVarUInt(agg_info->chunk_num, *compressed_buf);
    }
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
        initWriterOnce();    /// In case no chunks were written
        writer->flush();
        compressed_buf->finalize();
    }
    out.finalize();

    LOG_TEST(log, "Finished writing to stream {}, total rows: {}, bytes: {}", stream_name, rows_written, out.count());
}

}
