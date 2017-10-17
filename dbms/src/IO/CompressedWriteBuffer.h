#pragma once

#include <memory>

#include <Common/PODArray.h>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionSettings.h>


namespace DB
{

class CompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
private:
    WriteBuffer & out;
    CompressionSettings compression_settings;

    PODArray<char> compressed_buffer;

    void nextImpl() override;

public:
    CompressedWriteBuffer(
        WriteBuffer & out_,
        CompressionSettings compression_settings = CompressionSettings(),
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
