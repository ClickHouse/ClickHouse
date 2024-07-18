#include <IO/ZstdDeflatingWriteBuffer.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ZSTD_ENCODER_FAILED;
}

ZstdDeflatingWriteBuffer::ZstdDeflatingWriteBuffer(
    std::unique_ptr<WriteBuffer> out_, int compression_level, size_t buf_size, char * existing_memory, size_t alignment)
    : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment)
{
    cctx = ZSTD_createCCtx();
    if (cctx == nullptr)
        throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED, "zstd stream encoder init failed: zstd version: {}", ZSTD_VERSION_STRING);
    size_t ret = ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, compression_level);
    if (ZSTD_isError(ret))
        throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED,
                        "zstd stream encoder option setting failed: error code: {}; zstd version: {}",
                        ret, ZSTD_VERSION_STRING);
    ret = ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1);
    if (ZSTD_isError(ret))
        throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED,
                        "zstd stream encoder option setting failed: error code: {}; zstd version: {}",
                        ret, ZSTD_VERSION_STRING);

    input = {nullptr, 0, 0};
    output = {nullptr, 0, 0};
}

ZstdDeflatingWriteBuffer::~ZstdDeflatingWriteBuffer() = default;

void ZstdDeflatingWriteBuffer::flush(ZSTD_EndDirective mode)
{
    input.src = reinterpret_cast<unsigned char *>(working_buffer.begin());
    input.size = offset();
    input.pos = 0;

    try
    {
        bool ended = false;
        do
        {
            out->nextIfAtEnd();

            output.dst = reinterpret_cast<unsigned char *>(out->buffer().begin());
            output.size = out->buffer().size();
            output.pos = out->offset();

            size_t compression_result = ZSTD_compressStream2(cctx, &output, &input, mode);
            if (ZSTD_isError(compression_result))
                throw Exception(
                                ErrorCodes::ZSTD_ENCODER_FAILED,
                                "ZSTD stream encoding failed: error: '{}'; zstd version: {}",
                                ZSTD_getErrorName(compression_result), ZSTD_VERSION_STRING);

            out->position() = out->buffer().begin() + output.pos;

            bool everything_was_compressed = (input.pos == input.size);
            bool everything_was_flushed = compression_result == 0;

            ended = everything_was_compressed && everything_was_flushed;
        } while (!ended);
    }
    catch (...)
    {
        /// Do not try to write next time after exception.
        out->position() = out->buffer().begin();
        throw;
    }
}

void ZstdDeflatingWriteBuffer::nextImpl()
{
    if (offset())
        flush(ZSTD_e_flush);
}

void ZstdDeflatingWriteBuffer::finalizeBefore()
{
    flush(ZSTD_e_end);
}

void ZstdDeflatingWriteBuffer::finalizeAfter()
{
    try
    {
        size_t err = ZSTD_freeCCtx(cctx);
        /// This is just in case, since it is impossible to get an error by using this wrapper.
        if (unlikely(err))
            throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED, "ZSTD_freeCCtx failed: error: '{}'; zstd version: {}",
                            ZSTD_getErrorName(err), ZSTD_VERSION_STRING);
    }
    catch (...)
    {
        /// It is OK not to terminate under an error from ZSTD_freeCCtx()
        /// since all data already written to the stream.
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
