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
        size_t adaptive_buffer_initial_size = DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE,
        bool out_buffer_is_exclusive_ = false);

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

private:
    void nextImpl() override;

    /// Choose where the caller writes the next block's data.
    /// For the NONE codec we point the working buffer directly into `out` (leaving room for the
    /// checksum and header in front) so the data is written straight to the output buffer without
    /// an intermediate copy. Otherwise (or when `out` has no room) we use the owned buffer.
    void setupBufferForNextBlock();

    /// finalize call does not affect the out buffer.
    /// That is made in order to handle the use case when several CompressedWriteBuffers write to the one file.
    /// Usually the CompressedWriteBuffer does not own the out buffer.
    void finalizeImpl() override;
    /// cancel call cancels the out buffer.
    void cancelImpl() noexcept override;

    WriteBuffer & out;
    CompressionCodecPtr codec;

    /// True if this buffer is the only writer of `out` and writes to it strictly sequentially
    /// (nothing else moves out.position() between our blocks). Only then can the NONE codec write
    /// data directly into out's buffer ahead of finalizing the header (the zero-copy path).
    /// It must stay false when `out` is shared with other CompressedWriteBuffers and written
    /// interleaved (e.g. per-column streams in compact parts share one output buffer).
    bool out_buffer_is_exclusive;

    /// If true, the size of internal buffer will be exponentially increased up to
    /// adaptive_buffer_max_size after each nextImpl call. It can be used to avoid
    /// large buffer allocation when actual size of written data is small.
    bool use_adaptive_buffer_size;
    size_t adaptive_buffer_max_size;

    PODArray<char> compressed_buffer;

    /// True when the working buffer currently points directly into `out` (zero-copy NONE path),
    /// so nextImpl only has to write the header and checksum in place instead of copying the data.
    bool current_block_is_direct = false;
};

}
