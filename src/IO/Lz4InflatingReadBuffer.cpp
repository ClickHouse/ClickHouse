#include "Lz4InflatingReadBuffer.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LZ4_DECODER_FAILED;
}

Lz4InflatingReadBuffer::Lz4InflatingReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<ReadBuffer>(buf_size, existing_memory, alignment)
    , in(std::move(in_))
    , src_capacity(0)
    , dst_capacity(0)
    , in_available(0)
{
    ret = 1;


    dctx_status = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
    if (LZ4F_isError(dctx_status))
    {
        throw Exception(
            ErrorCodes::LZ4_DECODER_FAILED,
            "LZ4 failed create decompression context LZ4F_dctx. LZ4F version: {}. Error: {}",
            LZ4F_VERSION,
            LZ4F_getErrorName(dctx_status),
            ErrorCodes::LZ4_DECODER_FAILED);
    }
}

Lz4InflatingReadBuffer::~Lz4InflatingReadBuffer()
{
    LZ4F_freeDecompressionContext(dctx);
}

bool Lz4InflatingReadBuffer::nextImpl()
{
    if (eof)
        return false;


    if (!in_available)
    {
        in->nextIfAtEnd();
        in_available = in->buffer().end() - in->position();
        src = reinterpret_cast<void *>(in->position());
    }


    src_capacity = in_available;
    dst_capacity = internal_buffer.size();
    dst = reinterpret_cast<void *>(internal_buffer.begin());

    ret = LZ4F_decompress(dctx, dst, &dst_capacity, src, &src_capacity, /* LZ4F_decompressOptions_t */ nullptr);
    if (LZ4F_isError(ret)) {
        printf("Decompression error: %s\n", LZ4F_getErrorName(ret));
        throw Exception(
            ErrorCodes::LZ4_DECODER_FAILED,
            "LZ4 failed to fetch get info LZ4F_getFrameInfo. LZ4F version: {}. Error: {}",
            LZ4F_VERSION,
            LZ4F_getErrorName(ret),
            ErrorCodes::LZ4_DECODER_FAILED);
    }

    in->position() = in->buffer().begin() + src_capacity;
    working_buffer.resize(dst_capacity);

    in_available -= src_capacity;

    if (in->eof())
    {
        eof = true;
        return !working_buffer.empty();
    }

    return true;
}
}
