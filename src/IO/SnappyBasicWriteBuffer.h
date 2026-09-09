#pragma once

#include "config.h"

#if USE_SNAPPY
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>

namespace DB
{

/// Compresses the whole input into a single raw snappy block
/// (`varint(uncompressed_size) || compressed_payload`, as produced by one
/// `snappy::Compress` call) and writes it to the underlying buffer.
///
/// The public snappy API compresses a complete buffer at once, so this buffer
/// accumulates all written data and emits the compressed block on finalization;
/// memory usage is therefore proportional to the uncompressed size rather than
/// bounded by the buffer size. This is the protocol-specific format used e.g. by
/// the Prometheus remote protocol, as opposed to the standard snappy framing
/// format handled by `SnappyFramedWriteBuffer`.
///
/// This is the inverse of `SnappyBasicReadBuffer`.
class SnappyBasicWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    template <typename WriteBufferT>
    explicit SnappyBasicWriteBuffer(
        WriteBufferT && out_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr, /// NOLINT(readability-non-const-parameter)
        size_t alignment = 0,
        bool compress_empty_ = true)
        : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment) /// NOLINT(bugprone-move-forwarding-reference)
        , compress_empty(compress_empty_)
    {
    }

private:
    void nextImpl() override;

    void finalFlushBefore() override;

    /// Accumulates the entire uncompressed input until finalization.
    String uncompress_buffer;
    bool compress_empty = true;
};

}

#endif
