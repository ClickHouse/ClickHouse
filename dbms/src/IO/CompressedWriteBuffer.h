#pragma once

#include <memory>

#include <Common/PODArray.h>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressedStream.h>


namespace DB
{

class CompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
private:
    WriteBuffer & out;
    CompressionMethod method;

    PODArray<char> compressed_buffer;

    void nextImpl() override;

public:
    CompressedWriteBuffer(
        WriteBuffer & out_,
        CompressionMethod method_ = CompressionMethod::LZ4,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

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

    ~CompressedWriteBuffer() override;
};

}
