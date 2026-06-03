#pragma once

#include <Processors/ISink.h>
#include <Processors/Port.h>
#include <Common/Logger.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Formats/NativeWriter.h>

namespace DB
{

class NativeCompressedSink final : public ISink
{
public:
    NativeCompressedSink(SharedHeader header_, WriteBuffer & out_, const String & stream_name_)
        : ISink(std::move(header_))
        , stream_name(stream_name_)
        , out(out_)
        , rows_written(0)
    {
    }

    ~NativeCompressedSink() override;

    String getName() const override { return "NativeCompressedSink"; }

    void consume(Chunk chunk) override;

    void onFinish() override;

private:
    void initWriterOnce(const Chunk & chunk);

    const String stream_name;

    WriteBuffer & out;
    std::unique_ptr<CompressedWriteBuffer> compressed_buf;
    std::unique_ptr<NativeWriter> writer;
    size_t rows_written;
    LoggerPtr log = getLogger("NativeCompressedSink");
};

}
