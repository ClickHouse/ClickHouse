#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>

#include <zstd.h>


namespace DB
{

/// Performs stream compression using zstd library and writes compressed data to out_ WriteBuffer.
/// Main differences from ZstdDeflatingWriteBuffer:
/// 1) Allows to continue to write to the same output even if finalize() (or destructor) was not called, for example
///    when server was killed with 9 signal. Natively zstd doesn't support such feature because
///    ZSTD_decompressStream expect to see empty block at the end of each frame. There is not API function for it
///    so we just use HACK and add empty block manually on the first write (see addEmptyBlock). Maintainers of zstd
///    said that there is no risks of compatibility issues https://github.com/facebook/zstd/issues/2090#issuecomment-620158967.
/// 2) Doesn't support internal ZSTD check-summing, because ZSTD checksums written at the end of frame (frame epilogue).
///
class ZstdDeflatingAppendableWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    ZstdDeflatingAppendableWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level,
        bool append_to_existing_stream_, /// if true then out mustn't be empty
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~ZstdDeflatingAppendableWriteBuffer() override;

    void sync() override
    {
        next();
        out->sync();
    }

private:
    /// NOTE: will fill compressed data to the out.working_buffer, but will not call out.next method until the buffer is full
    void nextImpl() override;

    /// Write terminating ZSTD_e_end: empty block + frame epilogue. BTW it
    /// should be almost noop, because frame epilogue contains only checksums,
    /// and they are disabled for this buffer.
    /// Flush all pending data and write zstd footer to the underlying buffer.
    /// After the first call to this function, subsequent calls will have no effect and
    /// an attempt to write to this buffer will result in exception.
    void finalizeImpl() override;
    void finalizeBefore() override;
    void finalizeAfter() override;
    /// Adding zstd empty block to out.working_buffer
    void addEmptyBlock();

    /// We appending data to existing stream so on the first nextImpl call we
    /// will append empty block.
    bool append_to_existing_stream;
    ZSTD_CCtx * cctx;
    ZSTD_inBuffer input;
    ZSTD_outBuffer output;
    /// Flipped on the first nextImpl call
    bool first_write = true;
};

}
