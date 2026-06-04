#pragma once

#include "config.h"

#if USE_ARROW

#include <cstdint>
#include <cstddef>

namespace DB
{
class WriteBuffer;
}

namespace DB::ArrowIPC
{

/// Writes the Arrow IPC "encapsulated message" framing to a `WriteBuffer`:
///
///     <continuation = 0xFFFFFFFF> <metadata_size : int32 LE> <FlatBuffer metadata + padding> <body>
///
/// The metadata is zero-padded so the body begins 8-byte aligned, as required by the spec. The body
/// is expected to already be internally padded (each buffer to 8 bytes). The running byte offset is
/// tracked so the file writer can record `Block` positions for the footer.
class MessageWriter
{
public:
    explicit MessageWriter(WriteBuffer & out_) : out(out_) { }

    struct WrittenMessage
    {
        int64_t offset = 0;          /// where the message starts
        int32_t metadata_length = 0; /// bytes from the start up to the body (prefix + padded metadata)
        int64_t body_length = 0;
    };

    /// Writes one message and returns its location, for recording an Arrow file `Block`.
    WrittenMessage writeMessage(const uint8_t * metadata, size_t metadata_size, const char * body, size_t body_size);

    /// Writes the end-of-stream marker (continuation token followed by a zero length).
    void writeEOS();

    /// Writes raw bytes (e.g. the file magic), advancing the tracked offset.
    void writeRaw(const char * data, size_t size);

    int64_t offset() const { return bytes_written; }

private:
    WriteBuffer & out;
    int64_t bytes_written = 0;
};

}

#endif
