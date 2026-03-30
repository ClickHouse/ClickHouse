#pragma once

#include "config.h"

#if USE_SNAPPY
#include <IO/CompressedReadBufferWrapper.h>

namespace DB
{

/// Performs streaming decompression of the snappy framing format
/// (see https://github.com/google/snappy/blob/main/framing_format.txt)
/// and returns decompressed data to the caller.
///
/// This is the inverse of SnappyWriteBuffer.
class SnappyFramedReadBuffer : public CompressedReadBufferWrapper
{
public:
    explicit SnappyFramedReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

private:
    bool nextImpl() override;

    /// Read exactly `size` bytes from the underlying buffer into `dst`.
    /// If allow_partial_eof is true, returns false on clean EOF (0 bytes read at boundary).
    /// Otherwise (or on partial read), throws an exception.
    bool readExact(char * dst, size_t size, bool allow_partial_eof);

    /// Read and validate the stream identifier chunk.
    bool readStreamIdentifier();

    String decompress_buffer;
    bool identifier_read = false;
};

}

#endif
