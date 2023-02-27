#include <IO/Lz4InflatingReadBuffer.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LZ4_DECODER_FAILED;
}

Lz4InflatingReadBuffer::Lz4InflatingReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t buf_size, char * existing_memory, size_t alignment)
    : CompressedReadBufferWrapper(std::move(in_), buf_size, existing_memory, alignment)
    , in_data(nullptr)
    , out_data(nullptr)
    , in_available(0)
    , out_available(0)
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
    if (eof_flag)
        return false;

    bool need_more_input = false;
    size_t ret;

    do
    {
        if (!in_available)
        {
            in->nextIfAtEnd();
            in_available = in->buffer().end() - in->position();
        }

        in_data = reinterpret_cast<void *>(in->position());
        out_data = reinterpret_cast<void *>(internal_buffer.begin());

        out_available = internal_buffer.size();

        size_t bytes_read = in_available;
        size_t bytes_written = out_available;

        ret = LZ4F_decompress(dctx, out_data, &bytes_written, in_data, &bytes_read, /* LZ4F_decompressOptions_t */ nullptr);

        in_available -= bytes_read;
        out_available -= bytes_written;

        /// It may happen that we didn't get new uncompressed data
        /// (for example if we read the end of frame). Load new data
        /// in this case.
        need_more_input = bytes_written == 0;

        in->position() = in->buffer().end() - in_available;
    }
    while (need_more_input && !LZ4F_isError(ret) && !in->eof());

    working_buffer.resize(internal_buffer.size() - out_available);

    if (LZ4F_isError(ret))
        throw Exception(
            ErrorCodes::LZ4_DECODER_FAILED,
            "LZ4 decompression failed. LZ4F version: {}. Error: {}",
            LZ4F_VERSION,
            LZ4F_getErrorName(ret));

    if (in->eof())
    {
        eof_flag = true;
        return !working_buffer.empty();
    }

    return true;
}
}
