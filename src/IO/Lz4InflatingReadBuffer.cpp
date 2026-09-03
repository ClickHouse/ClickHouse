#include <IO/Lz4InflatingReadBuffer.h>
#include <IO/WithFileName.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LZ4_DECODER_FAILED;
}

Lz4InflatingReadBuffer::Lz4InflatingReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t buf_size, char * existing_memory, size_t alignment)
    : CompressedReadBufferWrapper(std::move(in_), buf_size, existing_memory, alignment)
{
    size_t ret = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);

    if (LZ4F_isError(ret))
        throw Exception(
            ErrorCodes::LZ4_DECODER_FAILED,
            "LZ4 failed create decompression context LZ4F_dctx. LZ4F version: {}. Error: {}",
            LZ4F_VERSION,
            LZ4F_getErrorName(ret));
}

Lz4InflatingReadBuffer::~Lz4InflatingReadBuffer()
{
    LZ4F_freeDecompressionContext(dctx);
}

bool Lz4InflatingReadBuffer::nextImpl()
{
    while (true)
    {
        if (!in_eof)
            in_eof = in->eof();

        void * in_data = reinterpret_cast<void *>(in->position());
        size_t in_available = in->available();
        chassert(in_available > 0 || in_eof);

        void * out_data = reinterpret_cast<void *>(internal_buffer.begin());
        size_t out_available = internal_buffer.size();
        chassert(out_available > 0);

        size_t bytes_read = in_available;
        size_t bytes_written = out_available;

        size_t ret = LZ4F_decompress(dctx, out_data, &bytes_written, in_data, &bytes_read, /* LZ4F_decompressOptions_t */ nullptr);

        if (LZ4F_isError(ret))
            throw Exception(
                ErrorCodes::LZ4_DECODER_FAILED,
                "LZ4 decompression failed. LZ4F version: {}. Error: {}{}",
                LZ4F_VERSION,
                LZ4F_getErrorName(ret),
                getExceptionEntryWithFileName(*in));

        in->position() += bytes_read;

        if (bytes_written > 0)
        {
            working_buffer.resize(bytes_written);
            return true;
        }

        if (bytes_read == 0)
        {
            chassert(in_eof);
            return false;
        }
    }
}
}
