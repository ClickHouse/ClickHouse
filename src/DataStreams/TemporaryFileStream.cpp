#include <DataStreams/TemporaryFileStream.h>
#include <DataStreams/NativeReader.h>
#include <DataStreams/NativeWriter.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <Core/ProtocolDefines.h>


namespace DB
{

/// To read the data that was flushed into the temporary data file.
TemporaryFileStream::TemporaryFileStream(const std::string & path)
    : file_in(path)
    , compressed_in(file_in)
    , block_in(std::make_unique<NativeReader>(compressed_in, DBMS_TCP_PROTOCOL_VERSION))
{}

TemporaryFileStream::TemporaryFileStream(const std::string & path, const Block & header_)
    : file_in(path)
    , compressed_in(file_in)
    , block_in(std::make_unique<NativeReader>(compressed_in, header_, 0))
{}

/// Flush data from input stream into file for future reading
void TemporaryFileStream::write(const std::string & path, const Block & header, QueryPipelineBuilder builder, const std::string & codec)
{
    WriteBufferFromFile file_buf(path);
    CompressedWriteBuffer compressed_buf(file_buf, CompressionCodecFactory::instance().get(codec, {}));
    NativeWriter output(compressed_buf, 0, header);

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PullingPipelineExecutor executor(pipeline);

    Block block;
    while (executor.pull(block))
        output.write(block);

    compressed_buf.finalize();
}

TemporaryFileLazySource::TemporaryFileLazySource(const std::string & path_, const Block & header_)
    : ISource(header_)
    , path(path_)
    , done(false)
{}

Chunk TemporaryFileLazySource::generate()
{
    if (done)
        return {};

    if (!stream)
        stream = std::make_unique<TemporaryFileStream>(path, header);

    auto block = stream->block_in->read();
    if (!block)
    {
        done = true;
        stream.reset();
    }
    return Chunk(block.getColumns(), block.rows());
}

}
