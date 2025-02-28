#include <Processors/Sources/NativeCompressedSource.h>
#include <Core/ProtocolDefines.h>

namespace DB
{

Chunk NativeCompressedSource::generate()
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
