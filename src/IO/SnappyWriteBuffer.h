#pragma once

#include "config.h"

#if USE_SNAPPY
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>

namespace DB
{

/// Performs streaming compression using the snappy framing format
/// (see https://github.com/google/snappy/blob/main/framing_format.txt)
/// and writes compressed data to the underlying buffer.
///
/// Each nextImpl call compresses the accumulated input as one snappy frame,
/// so memory usage is bounded by the buffer size (default DBMS_DEFAULT_BUFFER_SIZE).
class SnappyWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
    using Base = BufferWithOwnMemory<WriteBuffer>;
public:
    explicit SnappyWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    explicit SnappyWriteBuffer(
        WriteBuffer & out_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    explicit SnappyWriteBuffer(
        WriteBuffer * out_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    void finalizeImpl() override;

private:
    void nextImpl() override;
    void cancelImpl() noexcept override;

    /// Write the 10-byte stream identifier chunk.
    void writeStreamIdentifier();

    /// Compress and write one snappy framed chunk.
    void writeCompressedChunk(const char * data, size_t size);

    /// Write raw bytes to the underlying buffer.
    void writeRaw(const char * data, size_t size);

    WriteBuffer * out;
    std::unique_ptr<WriteBuffer> out_holder;

    String compress_buffer;
    bool header_written = false;
};

}

#endif
