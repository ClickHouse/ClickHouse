#include <Processors/Sinks/NativeCompressedSink.h>
#include <Core/ProtocolDefines.h>

namespace DB
{

void NativeCompressedSink::onStart()
{
    compressed_buf = std::make_unique<CompressedWriteBuffer>(out);
    writer = std::make_unique<NativeWriter>(*compressed_buf, DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS, input.getHeader());
}

void NativeCompressedSink::consume(Chunk chunk)
{
    Block block = input.getHeader().cloneWithColumns(chunk.getColumns());
    writer->write(block);
}

void NativeCompressedSink::onFinish()
{
    writer->flush();
    compressed_buf->finalize();
    out.finalize();
}

}
