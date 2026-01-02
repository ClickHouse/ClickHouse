#pragma once

#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <Formats/NativeReader.h>

namespace DB
{

/// Used only in MergeJoin
/// TODO: use `TemporaryDataOnDisk` instead
/// To read the data that was flushed into the temporary data file.
struct TemporaryFileStreamLegacy
{
    struct Stat
    {
        size_t compressed_bytes = 0;
        size_t uncompressed_bytes = 0;
    };

    ReadBufferFromFile file_in;
    CompressedReadBuffer compressed_in;
    std::unique_ptr<NativeReader> block_in;

    explicit TemporaryFileStreamLegacy(const std::string & path);
    TemporaryFileStreamLegacy(const std::string & path, const Block & header_);

    /// Flush data from input stream into file for future reading
    static Stat write(const std::string & path, const Block & header, QueryPipelineBuilder builder, const std::string & codec);
};

}
