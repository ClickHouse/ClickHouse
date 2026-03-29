#pragma once

#include "config.h"

#if USE_SNAPPY
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>

namespace DB
{

/// Performs streaming compression using the snappy framing format
/// (see https://github.com/google/snappy/blob/main/framing_format.txt)
/// and writes compressed data to the underlying buffer.
///
/// Each nextImpl call compresses the accumulated input as one snappy frame,
/// so memory usage is bounded by the buffer size (default DBMS_DEFAULT_BUFFER_SIZE).
class SnappyWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    template <typename WriteBufferT>
    explicit SnappyWriteBuffer(
        WriteBufferT && out_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr, /// NOLINT(readability-non-const-parameter)
        size_t alignment = 0)
        : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment) /// NOLINT(bugprone-move-forwarding-reference)
    {
    }

private:
    void nextImpl() override;

    void finalFlushBefore() override;

    /// Write the 10-byte stream identifier chunk.
    void writeStreamIdentifier();

    /// Compress and write one snappy framed chunk.
    void writeCompressedChunk(const char * data, size_t size);

    String compress_buffer;
    bool header_written = false;
};

}

#endif
