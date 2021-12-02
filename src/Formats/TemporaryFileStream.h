#pragma once

#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <Formats/NativeReader.h>

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

}
