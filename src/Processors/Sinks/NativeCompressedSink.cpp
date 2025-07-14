#include <Processors/Sinks/NativeCompressedSink.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteHelpers.h>

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

void NativeCompressedSink::onStart()
{
    if (input.getHeader().empty()) /// No input columns? (case of `SELECT count()`)
        return;

    compressed_buf = std::make_unique<CompressedWriteBuffer>(out);
    writer = std::make_unique<NativeWriter>(*compressed_buf, DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS, input.getSharedHeader());
}

void NativeCompressedSink::consume(Chunk chunk)
{
    rows_written += chunk.getNumRows();

    if (input.getHeader().empty()) /// Blocks without columns will not be written, we will write total rows count only at the end.
        return;

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
        writer->flush();
        compressed_buf->finalize();
    }
    out.finalize();
}

}
