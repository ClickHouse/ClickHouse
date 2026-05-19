#pragma once

#include <Processors/ISource.h>
#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeReader.h>
#include <Common/logger_useful.h>

namespace DB
{

class NativeCompressedSource final : public ISource
{
public:
    NativeCompressedSource(SharedHeader header_, std::unique_ptr<ReadBuffer> in_, const String & stream_name_)
        : ISource(std::move(header_))
        , stream_name(stream_name_)
        , in(std::move(in_))
    {
    }

    String getName() const override { return "NativeCompressedSource"; }

private:
    Chunk generate() override;

    const String stream_name;

    std::unique_ptr<ReadBuffer> in;
    std::unique_ptr<CompressedReadBuffer> compressed_buf;
    std::unique_ptr<NativeReader> reader;
    UInt64 stream_flags = 0;    /// Flags are read from the input stream once at start
    LoggerPtr log = getLogger("NativeCompressedSource");
};

}
