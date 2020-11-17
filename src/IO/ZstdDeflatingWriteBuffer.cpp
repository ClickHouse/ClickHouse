#include <IO/ZstdDeflatingWriteBuffer.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ZSTD_ENCODER_FAILED;
}

ZstdDeflatingWriteBuffer::ZstdDeflatingWriteBuffer(
    std::unique_ptr<WriteBuffer> out_, int compression_level, size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment), out(std::move(out_))
{
    cctx = ZSTD_createCCtx();
    if (cctx == nullptr)
        throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED, "zstd stream encoder init failed: zstd version: {}", ZSTD_VERSION_STRING);
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, compression_level);
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1);
}


ZstdDeflatingWriteBuffer::~ZstdDeflatingWriteBuffer()
{
    try
    {
        finish();

        ZSTD_freeCCtx(cctx);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ZstdDeflatingWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    bool last_chunk = hasPendingData();

    ZSTD_EndDirective mode = last_chunk ? ZSTD_e_end : ZSTD_e_flush;

    input.src = reinterpret_cast<unsigned char *>(working_buffer.begin());
    input.size = offset();
    input.pos = 0;

    bool finished = false;
    do
    {
        out->nextIfAtEnd();

        output.dst = reinterpret_cast<unsigned char *>(out->buffer().begin());
        output.size = out->buffer().size();
        output.pos = out->offset();


        size_t remaining = ZSTD_compressStream2(cctx, &output, &input, mode);
        out->position() = out->buffer().begin() + output.pos;
        finished = last_chunk ? (remaining == 0) : (input.pos == input.size);
    } while (!finished);

    if (last_chunk)
        flushed = true;
}

void ZstdDeflatingWriteBuffer::finish()
{
    if (flushed)
        return;

    next();

    out->nextIfAtEnd();

    input.src = reinterpret_cast<unsigned char *>(working_buffer.begin());
    input.size = offset();
    input.pos = 0;

    output.dst = reinterpret_cast<unsigned char *>(out->buffer().begin());
    output.size = out->buffer().size();
    output.pos = out->offset();

    size_t remaining = ZSTD_compressStream2(cctx, &output, &input, ZSTD_e_end);
    if (ZSTD_isError(remaining))
        throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED, "zstd stream encoder end failed: zstd version: {}", ZSTD_VERSION_STRING);
    out->position() = out->buffer().begin() + output.pos;
    flushed = true;
}

}