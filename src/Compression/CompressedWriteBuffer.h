#pragma once

#include <Common/PODArray.h>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Compression/ICompressionCodec.h>


namespace DB
{

class CompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit CompressedWriteBuffer(
        WriteBuffer & out_,
        CompressionCodecPtr codec_ = nullptr,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        bool use_adaptive_buffer_size_ = false,
        size_t adaptive_buffer_initial_size = DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE);

    /// Enables zero-copy `NONE` writes: a block is assembled in place inside `out` instead of
    /// being copied there. The caller guarantees that nothing else writes to or flushes `out`
    /// while this buffer is alive, the same contract `HashingWriteBuffer` and `ForkWriteBuffer`
    /// rely on. Call before writes or constructing a wrapper that captures the working buffer.
    /// A violation is detected in every build, including release: the block is not finished, the
    /// whole buffer chain is canceled and `LOGICAL_ERROR` is thrown, so a foreign write can never
    /// be committed as a valid-looking compressed block.
    void declareOutBufferExclusive();

    /// The amount of compressed data
    size_t getCompressedBytes()
    {
        nextIfAtEnd();
        return out.count();
    }

    /// How many uncompressed bytes were written to the buffer
    size_t getUncompressedBytes()
    {
        return count();
    }

    /// How many bytes are in the buffer (not yet compressed)
    size_t getRemainingBytes()
    {
        nextIfAtEnd();
        return offset();
    }

    CompressionCodecPtr getCodec() const { return codec; }

    void setCodec(CompressionCodecPtr codec_);

    /// Returns the buffer that compressed data is written into.
    WriteBuffer * getNestedBuffer() const { return &out; }

private:
    void nextImpl() override;

    /// Select the direct or owned buffer for the next block.
    void setupBufferForNextBlock();

    /// finalize call does not affect the out buffer.
    /// That is made in order to handle the use case when several CompressedWriteBuffers write to the one file.
    /// Usually the CompressedWriteBuffer does not own the out buffer.
    void finalizeImpl() override;
    /// cancel call cancels the out buffer.
    void cancelImpl() noexcept override;

    WriteBuffer & out;
    CompressionCodecPtr codec;

    /// Current block size, growing up to adaptive_buffer_max_size.
    size_t block_size;

    PODArray<char> compressed_buffer;

    /// See declareOutBufferExclusive.
    bool out_buffer_is_exclusive = false;

    /// Whether the current block is written in place inside `out` rather than into `memory`.
    bool block_is_written_in_place = false;
};

}
