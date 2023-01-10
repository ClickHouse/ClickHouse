#include <IO/ZstdInflatingReadBuffer.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ZSTD_DECODER_FAILED;
}

ZstdInflatingReadBuffer::ZstdInflatingReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t buf_size, char * existing_memory, size_t alignment, int zstd_window_log_max)
    : CompressedReadBufferWrapper(std::move(in_), buf_size, existing_memory, alignment)
{
    dctx = ZSTD_createDCtx();
    input = {nullptr, 0, 0};
    output = {nullptr, 0, 0};

    if (dctx == nullptr)
    {
        throw Exception(ErrorCodes::ZSTD_DECODER_FAILED, "zstd_stream_decoder init failed: zstd version: {}", ZSTD_VERSION_STRING);
    }

    size_t ret = ZSTD_DCtx_setParameter(dctx, ZSTD_d_windowLogMax, zstd_window_log_max);
    if (ZSTD_isError(ret))
    {
        throw Exception(ErrorCodes::ZSTD_DECODER_FAILED, "zstd_stream_decoder init failed: {}", ZSTD_getErrorName(ret));
    }
}

ZstdInflatingReadBuffer::~ZstdInflatingReadBuffer()
{
    ZSTD_freeDCtx(dctx);
}

bool ZstdInflatingReadBuffer::nextImpl()
{
    do
    {
        // If it is known that end of file was reached, return false
        if (eof_flag)
            return false;

        /// If end was reached, get next part
        if (input.pos >= input.size)
        {
            in->nextIfAtEnd();
            input.src = reinterpret_cast<unsigned char *>(in->position());
            input.pos = 0;
            input.size = in->buffer().end() - in->position();
        }

        /// fill output
        output.dst = reinterpret_cast<unsigned char *>(internal_buffer.begin());
        output.size = internal_buffer.size();
        output.pos = 0;

        /// Decompress data and check errors.
        size_t ret = ZSTD_decompressStream(dctx, &output, &input);
        if (ZSTD_isError(ret))
            throw Exception(
                ErrorCodes::ZSTD_DECODER_FAILED, "Zstd stream encoding failed: error '{}'; zstd version: {}", ZSTD_getErrorName(ret), ZSTD_VERSION_STRING);

        /// Check that something has changed after decompress (input or output position)
        assert(in->eof() || output.pos > 0 || in->position() < in->buffer().begin() + input.pos);

        /// move position to the end of read data
        in->position() = in->buffer().begin() + input.pos;
        working_buffer.resize(output.pos);

        /// If end of file is reached, fill eof variable and return true if there is some data in buffer, otherwise return false
        if (in->eof())
        {
            eof_flag = true;
            return !working_buffer.empty();
        }
        /// It is possible, that input buffer is not at eof yet, but nothing was decompressed in current iteration.
        /// But there are cases, when such behaviour is not allowed - i.e. if input buffer is not eof, then
        /// it has to be guaranteed that working_buffer is not empty. So if it is empty, continue.
    } while (output.pos == 0);

    return true;
}

}
