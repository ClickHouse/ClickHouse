#pragma once

#include <Processors/ISink.h>
#include <Processors/Port.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Formats/NativeWriter.h>

namespace DB
{

class NativeCompressedSink final : public ISink
{
public:
    NativeCompressedSink(Block header_, WriteBuffer & out_)
        : ISink(std::move(header_))
        , out(out_)
        , rows_written(0)
    {
    }

    ~NativeCompressedSink() override;

    String getName() const override { return "NativeCompressedSink"; }

private:
    void onStart() override;

    void consume(Chunk chunk) override;

    void onFinish() override;

    WriteBuffer & out;
    std::unique_ptr<CompressedWriteBuffer> compressed_buf;
    std::unique_ptr<NativeWriter> writer;
    size_t rows_written;
};

}
