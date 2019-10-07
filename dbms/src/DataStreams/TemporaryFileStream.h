#pragma once

#include <Common/ClickHouseRevision.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>

namespace DB
{

/// To read the data that was flushed into the temporary data file.
struct TemporaryFileStream
{
    ReadBufferFromFile file_in;
    CompressedReadBuffer compressed_in;
    BlockInputStreamPtr block_in;

    TemporaryFileStream(const std::string & path)
        : file_in(path)
        , compressed_in(file_in)
        , block_in(std::make_shared<NativeBlockInputStream>(compressed_in, ClickHouseRevision::get()))
    {}

    TemporaryFileStream(const std::string & path, const Block & header_)
        : file_in(path)
        , compressed_in(file_in)
        , block_in(std::make_shared<NativeBlockInputStream>(compressed_in, header_, 0))
    {}
};

}
