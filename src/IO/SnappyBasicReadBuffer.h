#pragma once

#include "config.h"

#if USE_SNAPPY

#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{

/// Decompresses a raw snappy block: the whole input is a single buffer produced
/// by one `snappy::Compress` call (`varint(uncompressed_size) || compressed_payload`).
/// This is the protocol-specific format used e.g. by the Prometheus remote protocol,
/// as opposed to the standard snappy framing format handled by `SnappyFramedReadBuffer`.
///
/// This is the inverse of `SnappyBasicWriteBuffer`.
class SnappyBasicReadBuffer : public BufferWithOwnMemory<SeekableReadBuffer>
{
public:
    explicit SnappyBasicReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~SnappyBasicReadBuffer() override;

    bool nextImpl() override;
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

private:
    std::unique_ptr<ReadBuffer> in;
    String compress_buffer;
    String uncompress_buffer;
};

}
#endif
