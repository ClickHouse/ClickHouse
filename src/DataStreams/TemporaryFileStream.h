#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/copyData.h>
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
    static void write(const std::string & path, const Block & header, IBlockInputStream & input,
                      std::atomic<bool> * is_cancelled, const std::string & codec)
    {
        WriteBufferFromFile file_buf(path);
        CompressedWriteBuffer compressed_buf(file_buf, CompressionCodecFactory::instance().get(codec, {}));
        NativeBlockOutputStream output(compressed_buf, 0, header);
        copyData(input, output, is_cancelled);
        compressed_buf.finalize();
    }
};

class TemporaryFileLazyInputStream : public IBlockInputStream
{
public:
    TemporaryFileLazyInputStream(const std::string & path_, const Block & header_)
        : path(path_)
        , header(header_)
        , done(false)
    {}

    String getName() const override { return "TemporaryFile"; }
    Block getHeader() const override { return header; }
    void readSuffix() override {}

protected:
    Block readImpl() override
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
        return block;
    }

private:
    const std::string path;
    Block header;
    bool done;
    std::unique_ptr<TemporaryFileStream> stream;
};

}
