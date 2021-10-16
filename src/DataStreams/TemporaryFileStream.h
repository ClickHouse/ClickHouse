#pragma once

#include <Processors/ISource.h>
#include <Processors/QueryPipelineBuilder.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <DataStreams/NativeReader.h>

namespace DB
{

/// To read the data that was flushed into the temporary data file.
struct TemporaryFileStream
{
    ReadBufferFromFile file_in;
    CompressedReadBuffer compressed_in;
    std::unique_ptr<NativeReader> block_in;

    explicit TemporaryFileStream(const std::string & path);
    TemporaryFileStream(const std::string & path, const Block & header_);

    /// Flush data from input stream into file for future reading
    static void write(const std::string & path, const Block & header, QueryPipelineBuilder builder, const std::string & codec);
};


class TemporaryFileLazySource : public ISource
{
public:
    TemporaryFileLazySource(const std::string & path_, const Block & header_);
    String getName() const override { return "TemporaryFileLazySource"; }

protected:
    Chunk generate() override;

private:
    const std::string path;
    Block header;
    bool done;

    std::unique_ptr<TemporaryFileStream> stream;
};

}
