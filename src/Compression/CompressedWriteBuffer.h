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

    /// Declares that this buffer is the sole writer of `out`, enabling the zero-copy path for the
    /// NONE codec: the working buffer points directly into `out`, and nextImpl fills the checksum
    /// and the header in front of the data in place instead of copying the payload (enforced:
    /// nextImpl throws LOGICAL_ERROR if out.position() has moved). Call before writing any data
    /// and before anything captures the working buffer (e.g. a wrapping HashingWriteBuffer).
    /// Must not be used when `out` is shared, e.g. by the per-column streams of compact parts.
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

private:
    void nextImpl() override;

    /// Choose where the caller writes the next block: directly into `out` (zero-copy NONE path)
    /// or into the owned buffer.
    void setupBufferForNextBlock();

    /// finalize call does not affect the out buffer.
    /// That is made in order to handle the use case when several CompressedWriteBuffers write to the one file.
    /// Usually the CompressedWriteBuffer does not own the out buffer.
    void finalizeImpl() override;
    /// cancel call cancels the out buffer.
    void cancelImpl() noexcept override;

    WriteBuffer & out;
    CompressionCodecPtr codec;

    /// If true, the size of internal buffer will be exponentially increased up to
    /// adaptive_buffer_max_size after each nextImpl call. It can be used to avoid
    /// large buffer allocation when actual size of written data is small.
    bool use_adaptive_buffer_size;
    size_t adaptive_buffer_max_size;

    /// The size of the block that is currently being filled. It equals adaptive_buffer_max_size,
    /// unless the adaptive buffer size is used, in which case it starts small and grows up to it.
    size_t block_size;

    PODArray<char> compressed_buffer;

    /// See declareOutBufferExclusive.
    bool out_buffer_is_exclusive = false;

    /// Zero-copy NONE path: where out.position() stood when the working buffer was pointed into
    /// `out`, so nextImpl can check it has not moved. nullptr when the working buffer is the owned one.
    char * expected_out_position = nullptr;
};

}
