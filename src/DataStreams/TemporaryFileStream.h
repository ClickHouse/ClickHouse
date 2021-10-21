#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

namespace DB
{

/// To read the data that was flushed into the temporary data file.
struct TemporaryFileStream
{
    ReadBufferFromFile file_in;
    CompressedReadBuffer compressed_in;
    BlockInputStreamPtr block_in;

    explicit TemporaryFileStream(const std::string & path)
        : file_in(path)
        , compressed_in(file_in)
        , block_in(std::make_shared<NativeBlockInputStream>(compressed_in, DBMS_TCP_PROTOCOL_VERSION))
    {}

    TemporaryFileStream(const std::string & path, const Block & header_)
        : file_in(path)
        , compressed_in(file_in)
        , block_in(std::make_shared<NativeBlockInputStream>(compressed_in, header_, 0))
    {}

    /// Flush data from input stream into file for future reading
    static void write(const std::string & path, const Block & header, QueryPipeline pipeline, const std::string & codec)
    {
        WriteBufferFromFile file_buf(path);
        CompressedWriteBuffer compressed_buf(file_buf, CompressionCodecFactory::instance().get(codec, {}));
        NativeBlockOutputStream output(compressed_buf, 0, header);

        PullingPipelineExecutor executor(pipeline);

        output.writePrefix();

        Block block;
        while (executor.pull(block))
            output.write(block);

        output.writeSuffix();
        compressed_buf.finalize();
    }
};

class TemporaryFileLazySource : public ISource
{
public:
    TemporaryFileLazySource(const std::string & path_, const Block & header_)
        : ISource(header_)
        , path(path_)
        , done(false)
    {}

    String getName() const override { return "TemporaryFileLazySource"; }

protected:
    Chunk generate() override
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

private:
    const std::string path;
    Block header;
    bool done;
    std::unique_ptr<TemporaryFileStream> stream;
};

}
