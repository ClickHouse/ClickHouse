#include <IO/ZstdDeflatingAppendableWriteBuffer.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromFile.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ZSTD_ENCODER_FAILED;
}

ZstdDeflatingAppendableWriteBuffer::ZstdDeflatingAppendableWriteBuffer(
    std::unique_ptr<WriteBufferFromFile> out_,
    int compression_level,
    bool append_to_existing_file_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : BufferWithOwnMemory(buf_size, existing_memory, alignment)
    , out(std::move(out_))
    , append_to_existing_file(append_to_existing_file_)
{
    cctx = ZSTD_createCCtx();
    if (cctx == nullptr)
        throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED, "zstd stream encoder init failed: zstd version: {}", ZSTD_VERSION_STRING);
    size_t ret = ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, compression_level);
    if (ZSTD_isError(ret))
        throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED, "zstd stream encoder option setting failed: error code: {}; zstd version: {}", ret, ZSTD_VERSION_STRING);

    input = {nullptr, 0, 0};
    output = {nullptr, 0, 0};
}

void ZstdDeflatingAppendableWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    input.src = reinterpret_cast<unsigned char *>(working_buffer.begin());
    input.size = offset();
    input.pos = 0;

    if (first_write && append_to_existing_file && isNeedToAddEmptyBlock())
    {
        addEmptyBlock();
        first_write = false;
    }

    try
    {
        bool ended = false;
        do
        {
            out->nextIfAtEnd();

            output.dst = reinterpret_cast<unsigned char *>(out->buffer().begin());
            output.size = out->buffer().size();
            output.pos = out->offset();

            size_t compression_result = ZSTD_compressStream2(cctx, &output, &input, ZSTD_e_flush);
            if (ZSTD_isError(compression_result))
                throw Exception(
                    ErrorCodes::ZSTD_ENCODER_FAILED, "Zstd stream encoding failed: error code: {}; zstd version: {}", ZSTD_getErrorName(compression_result), ZSTD_VERSION_STRING);

            first_write = false;
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

ZstdDeflatingAppendableWriteBuffer::~ZstdDeflatingAppendableWriteBuffer()
{
    finalize();
}

void ZstdDeflatingAppendableWriteBuffer::finalizeImpl()
{
    if (first_write)
    {
        /// To free cctx
        finalizeZstd();
        /// Nothing was written
    }
    else
    {
        try
        {
            finalizeBefore();
            out->finalize();
            finalizeAfter();
        }
        catch (...)
        {
            /// Do not try to flush next time after exception.
            out->position() = out->buffer().begin();
            throw;
        }
    }
}

void ZstdDeflatingAppendableWriteBuffer::finalizeBefore()
{
    next();

    out->nextIfAtEnd();

    input.src = reinterpret_cast<unsigned char *>(working_buffer.begin());
    input.size = offset();
    input.pos = 0;

    output.dst = reinterpret_cast<unsigned char *>(out->buffer().begin());
    output.size = out->buffer().size();
    output.pos = out->offset();

    /// Actually we can use ZSTD_e_flush here and add empty termination
    /// block on each new buffer creation for non-empty file unconditionally (without isNeedToAddEmptyBlock).
    /// However ZSTD_decompressStream is able to read non-terminated frame (we use it in reader buffer),
    /// but console zstd utility cannot.
    size_t remaining = ZSTD_compressStream2(cctx, &output, &input, ZSTD_e_end);
    while (remaining != 0)
    {
        if (ZSTD_isError(remaining))
            throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED, "Zstd stream encoder end failed: error: '{}' zstd version: {}", ZSTD_getErrorName(remaining), ZSTD_VERSION_STRING);

        remaining = ZSTD_compressStream2(cctx, &output, &input, ZSTD_e_end);
    }
    out->position() = out->buffer().begin() + output.pos;
}

void ZstdDeflatingAppendableWriteBuffer::finalizeAfter()
{
    finalizeZstd();
}

void ZstdDeflatingAppendableWriteBuffer::finalizeZstd()
{
    try
    {
        int err = ZSTD_freeCCtx(cctx);
        /// This is just in case, since it is impossible to get an error by using this wrapper.
        if (unlikely(err))
            throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED, "ZSTD_freeCCtx failed: error: '{}'; zstd version: {}", ZSTD_getErrorName(err), ZSTD_VERSION_STRING);
    }
    catch (...)
    {
        /// It is OK not to terminate under an error from ZSTD_freeCCtx()
        /// since all data already written to the stream.
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ZstdDeflatingAppendableWriteBuffer::addEmptyBlock()
{
    /// HACK: https://github.com/facebook/zstd/issues/2090#issuecomment-620158967

    if (out->buffer().size() - out->offset() < ZSTD_CORRECT_TERMINATION_LAST_BLOCK.size())
        out->next();

    std::memcpy(out->buffer().begin() + out->offset(),
                ZSTD_CORRECT_TERMINATION_LAST_BLOCK.data(), ZSTD_CORRECT_TERMINATION_LAST_BLOCK.size());

    out->position() = out->buffer().begin() + out->offset() + ZSTD_CORRECT_TERMINATION_LAST_BLOCK.size();
}


bool ZstdDeflatingAppendableWriteBuffer::isNeedToAddEmptyBlock()
{
    ReadBufferFromFile reader(out->getFileName());
    auto fsize = reader.size();
    if (fsize > 3)
    {
        std::array<char, 3> result;
        reader.seek(fsize - 3, SEEK_SET);
        reader.readStrict(result.data(), 3);

        /// If we don't have correct block in the end, then we need to add it manually.
        /// NOTE: maybe we can have the same bytes in case of data corruption/unfinished write.
        /// But in this case file still corrupted and we have to remove it.
        return result != ZSTD_CORRECT_TERMINATION_LAST_BLOCK;
    }
    else if (fsize > 0)
    {
        throw Exception(
            ErrorCodes::ZSTD_ENCODER_FAILED,
            "Trying to write to non-empty file '{}' with tiny size {}. It can lead to data corruption",
            out->getFileName(), fsize);
    }
    return false;
}

}
