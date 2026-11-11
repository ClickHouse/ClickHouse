#include <Processors/Sources/NativeCompressedSource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Core/ProtocolDefines.h>
#include <IO/ReadHelpers.h>
#include <Common/logger_useful.h>

namespace DB
{

Chunk NativeCompressedSource::generate()
{
    if (output.getHeader().empty())    /// No output columns? (case of `SELECT count()`)
    {
        if (!in)
            return {};

        /// We must read the count of rows.
        size_t total_rows = 0;
        readVarUInt(total_rows, *in);
        in.reset(); /// Nothing more to read.
        return Chunk(Columns{}, total_rows);
    }
    else
    {
        if (!reader)
        {
            compressed_buf = std::make_unique<CompressedReadBuffer>(*in);
            reader = std::make_unique<NativeReader>(*compressed_buf, output.getHeader(), DBMS_TCP_PROTOCOL_VERSION);
        }

        /// Each block is prefixed with a per-block flag, and a chunk_num when it carries aggregation
        /// metadata. At end of stream there is no prefix, so stop here, the same way NativeReader::read
        /// stops on eof.
        if (compressed_buf->eof())
            return {};

        UInt64 block_flags = 0;
        readVarUInt(block_flags, *compressed_buf);
        const bool has_aggregated_chunk_info = (block_flags & 1);

        UInt64 chunk_num = 0;
        if (has_aggregated_chunk_info)
            readVarUInt(chunk_num, *compressed_buf);

        Block block = reader->read();

        LOG_TEST(log, "Read chunk with {} rows from stream {}", block.rows(), stream_name);

        Chunk result(block.getColumns(), block.rows());
        if (has_aggregated_chunk_info)
        {
            auto info = std::make_shared<AggregatedChunkInfo>();
            info->bucket_num = block.info.bucket_num;
            info->is_overflows = block.info.is_overflows;
            info->out_of_order_buckets = block.info.out_of_order_buckets;
            info->chunk_num = chunk_num;
            result.getChunkInfos().add(std::move(info));
        }
        return result;
    }
}

}
