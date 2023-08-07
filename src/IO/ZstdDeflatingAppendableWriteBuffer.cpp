#include <IO/ZstdDeflatingAppendableWriteBuffer.h>
#include <Common/Exception.h>
#include "IO/ReadBufferFromFileBase.h"
#include <IO/ReadBufferFromFile.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ZSTD_ENCODER_FAILED;
}

ZstdDeflatingAppendableWriteBuffer::ZstdDeflatingAppendableWriteBuffer(
    std::unique_ptr<WriteBuffer> out_,
    int compression_level,
    bool append_to_existing_file_,
    std::function<std::unique_ptr<ReadBufferFromFileBase>()> read_buffer_creator_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment)
    , read_buffer_creator(std::move(read_buffer_creator_))
    , append_to_existing_file(append_to_existing_file_)
{
    cctx = ZSTD_createCCtx();
    if (cctx == nullptr)
        throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED, "ZSTD stream encoder init failed: ZSTD version: {}", ZSTD_VERSION_STRING);
    size_t ret = ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, compression_level);
    if (ZSTD_isError(ret))
        throw Exception(ErrorCodes::ZSTD_ENCODER_FAILED,
                        "ZSTD stream encoder option setting failed: error code: {}; zstd version: {}",
                        ret, ZSTD_VERSION_STRING);

    input = {nullptr, 0, 0};
    output = {nullptr, 0, 0};
}

void ZstdDeflatingAppendableWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    if (first_write && append_to_existing_file && isNeedToAddEmptyBlock())
        addEmptyBlock();
    first_write = false;

    flush(ZSTD_e_flush);
}

void ZstdDeflatingAppendableWriteBuffer::flush(ZSTD_EndDirective mode)
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
                                "ZSTD stream decoding failed: error code: {}; ZSTD version: {}",
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

ZstdDeflatingAppendableWriteBuffer::~ZstdDeflatingAppendableWriteBuffer()
{
    finalize();
}

void ZstdDeflatingAppendableWriteBuffer::finalizeBefore()
{
    if (first_write && offset() == 0)
        return;

    /// This may look redundant given the flush() below, but it's not: we have to flush any pending
    /// data using ZSTD_e_flush rather than ZSTD_e_end, otherwise the resulting frame won't necessarily
    /// be compatible with ZSTD_CORRECT_TERMINATION_LAST_BLOCK.
    next();

    /// Actually we can use ZSTD_e_flush here and add empty termination block on each new buffer
    /// creation for non-empty file unconditionally (without isNeedToAddEmptyBlock).
    /// However ZSTD_decompressStream is able to read non-terminated frame (we use it in reader buffer),
    /// but console zstd utility cannot.
    flush(ZSTD_e_end);
}

void ZstdDeflatingAppendableWriteBuffer::finalizeAfter()
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

void ZstdDeflatingAppendableWriteBuffer::addEmptyBlock()
{
    /// HACK: https://github.com/facebook/zstd/issues/2090#issuecomment-620158967
    out->write(ZSTD_CORRECT_TERMINATION_LAST_BLOCK.data(), ZSTD_CORRECT_TERMINATION_LAST_BLOCK.size());
}


bool ZstdDeflatingAppendableWriteBuffer::isNeedToAddEmptyBlock()
{
    auto reader = read_buffer_creator();
    auto fsize = reader->getFileSize();
    if (fsize > 3)
    {
        std::array<char, 3> result;
        reader->seek(fsize - 3, SEEK_SET);
        reader->readStrict(result.data(), 3);

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
            reader->getFileName(), fsize);
    }
    return false;
}

}
