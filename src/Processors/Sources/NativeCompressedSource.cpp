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
            readVarUInt(stream_flags, *in);
            compressed_buf = std::make_unique<CompressedReadBuffer>(*in);
            reader = std::make_unique<NativeReader>(*compressed_buf, output.getHeader(), DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS);
        }

        Block block = reader->read();

        LOG_TEST(log, "Read chunk with {} rows from stream {}", block.rows(), stream_name);

        Chunk result(block.getColumns(), block.rows());
        const bool has_aggregated_chunk_info = (stream_flags & 1);
        if (has_aggregated_chunk_info)
        {
            auto info = std::make_shared<AggregatedChunkInfo>();
            info->bucket_num = block.info.bucket_num;
            info->is_overflows = block.info.is_overflows;
            result.getChunkInfos().add(std::move(info));
        }
        return result;
    }
}

}
