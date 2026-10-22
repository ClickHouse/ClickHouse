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

    /// Blocks that the codec does not compress well are written with codec `NONE`,
    /// so reading them does not pay for decompression. Must not be used with an encrypting codec.
    void storeIncompressibleBlocksUncompressed();

    /// Returns the buffer that compressed data is written into.
    WriteBuffer * getNestedBuffer() const { return &out; }

private:
    void nextImpl() override;
    /// finalize call does not affect the out buffer.
    /// That is made in order to handle the use case when several CompressedWriteBuffers write to the one file.
    /// Usually the CompressedWriteBuffer does not own the out buffer.
    void finalizeImpl() override;
    /// cancel call cancels the out buffer.
    void cancelImpl() noexcept override;

    WriteBuffer & out;
    CompressionCodecPtr codec;
    /// Codec `NONE` if `storeIncompressibleBlocksUncompressed` was called.
    CompressionCodecPtr uncompressed_codec;

    PODArray<char> compressed_buffer;
};

}
