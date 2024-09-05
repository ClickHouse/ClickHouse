#pragma once

#include <memory>

#include <Common/PODArray.h>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>


namespace DB
{

class CompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit CompressedWriteBuffer(
        WriteBuffer & out_,
        CompressionCodecPtr codec_ = CompressionCodecFactory::instance().getDefaultCodec(),
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    ~CompressedWriteBuffer() override;

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

private:
    void nextImpl() override;
    /// finalize call does not affect the out buffer.
    /// That is made in order to handle the usecase when several CompressedWriteBuffer's write to the one file
    void finalizeImpl() override;
    /// cancel call canecels the out buffer
    void cancelImpl() noexcept override;

    WriteBuffer & out;
    CompressionCodecPtr codec;

    PODArray<char> compressed_buffer;
};

}
