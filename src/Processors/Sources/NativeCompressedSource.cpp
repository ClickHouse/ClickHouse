#include <Processors/Sources/NativeCompressedSource.h>
#include <Core/ProtocolDefines.h>
#include <IO/ReadHelpers.h>

namespace DB
{

Chunk NativeCompressedSource::generate()
{
    if (!output.getHeader())    /// No output columns? (case of `SELECT count()`)
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
            reader = std::make_unique<NativeReader>(*compressed_buf, output.getHeader(), DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS);
        }
        Block block = reader->read();

        return Chunk(block.getColumns(), block.rows());
    }
}

}
