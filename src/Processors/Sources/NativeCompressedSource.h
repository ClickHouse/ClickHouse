#pragma once

#include <Processors/ISource.h>
#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeReader.h>

namespace DB
{

class NativeCompressedSource final : public ISource
{
public:
    NativeCompressedSource(Block header_, std::unique_ptr<ReadBuffer> in_)
        : ISource(std::move(header_))
        , in(std::move(in_))
    {
    }

    String getName() const override { return "NativeCompressedSource"; }

private:
    Chunk generate() override;

    std::unique_ptr<ReadBuffer> in;
    std::unique_ptr<CompressedReadBuffer> compressed_buf;
    std::unique_ptr<NativeReader> reader;
};

}
