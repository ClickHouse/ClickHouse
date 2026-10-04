#pragma once

#include "config.h"

#if USE_SNAPPY
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>

namespace DB
{

/// Performs streaming compression using the Hadoop-snappy block format
/// (see HadoopSnappyReadBuffer.h for the format description) and writes the
/// compressed data to the underlying buffer.
///
/// Each nextImpl call emits the accumulated input as one or more Hadoop-snappy
/// blocks (a single subblock per block), so memory usage is bounded by the
/// buffer size (default DBMS_DEFAULT_BUFFER_SIZE). The produced stream is
/// readable by HadoopSnappyReadBuffer and by Hadoop's own snappy codec.
class HadoopSnappyWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    template <typename WriteBufferT>
    explicit HadoopSnappyWriteBuffer(
        WriteBufferT && out_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr, /// NOLINT(readability-non-const-parameter)
        size_t alignment = 0,
        [[maybe_unused]] bool compress_empty_ = true) /// Empty input is already a valid (empty) Hadoop-snappy stream.
        : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment) /// NOLINT(bugprone-move-forwarding-reference)
    {
    }

private:
    void nextImpl() override;

    /// Compress and write one Hadoop-snappy block (a single subblock).
    void writeBlock(const char * data, size_t size);

    String compress_buffer;
};

}

#endif
