#include <IO/ZstdDeflatingWriteBuffer.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ZSTD_ENCODER_FAILED;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

static void setZstdParameter(ZSTD_CCtx * cctx, ZSTD_cParameter param, int value)
{
    auto ret = ZSTD_CCtx_setParameter(cctx, param, value);
    if (ZSTD_isError(ret))
        throw Exception(
            ErrorCodes::ZSTD_ENCODER_FAILED,
            "zstd stream encoder option setting failed: error code: {}; zstd version: {}",
            ret,
            ZSTD_VERSION_STRING);
}

void ZstdDeflatingWriteBuffer::initialize(int compression_level, int window_log)
{
    cctx = ZSTD_createCCtx();
    if (cctx == nullptr)
        throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED, "zstd stream encoder init failed: zstd version: {}", ZSTD_VERSION_STRING);
    setZstdParameter(cctx, ZSTD_c_compressionLevel, compression_level);

    if (window_log > 0)
    {
        ZSTD_bounds window_log_bounds = ZSTD_cParam_getBounds(ZSTD_c_windowLog);
        if (ZSTD_isError(window_log_bounds.error))
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "ZSTD windowLog parameter is not supported {}",
                std::string(ZSTD_getErrorName(window_log_bounds.error)));
        if (window_log > window_log_bounds.upperBound || window_log < window_log_bounds.lowerBound)
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                            "ZSTD codec can't have window log more than {} and lower than {}, given {}",
                            toString(window_log_bounds.upperBound),
                            toString(window_log_bounds.lowerBound), toString(window_log));
        setZstdParameter(cctx, ZSTD_c_enableLongDistanceMatching, 1);
        setZstdParameter(cctx, ZSTD_c_windowLog, window_log);
    }

    setZstdParameter(cctx, ZSTD_c_checksumFlag, 1);

    input = {nullptr, 0, 0};
    output = {nullptr, 0, 0};
}

ZstdDeflatingWriteBuffer::~ZstdDeflatingWriteBuffer()
{
    if (cctx)
        ZSTD_freeCCtx(cctx);
}

void ZstdDeflatingWriteBuffer::flush(ZSTD_EndDirective mode)
{
    input.src = reinterpret_cast<unsigned char *>(working_buffer.begin());
    input.size = offset();
    input.pos = 0;

    try
    {
        size_t out_offset = out->offset();
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

        total_out += out->offset() - out_offset;
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
    /// Don't write out if no data was ever compressed
    if (!compress_empty && total_out == 0)
        return;
    flush(ZSTD_e_end);
}

void ZstdDeflatingWriteBuffer::finalizeAfter()
{
    try
    {
        size_t err = ZSTD_freeCCtx(cctx);
        cctx = nullptr;
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
