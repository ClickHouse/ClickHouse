#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <Common/PODArray.h>

namespace DB
{
class ReadBuffer;
}

namespace DB::ArrowIPC
{

/// Reads the Arrow IPC "encapsulated message" framing from a `ReadBuffer`:
///
///     <continuation = 0xFFFFFFFF> <metadata_size : int32 LE> <FlatBuffer Message + padding> <body>
///
/// (the legacy pre-0.15.0 framing omits the continuation token and starts directly with the size).
/// The metadata FlatBuffer is read into an owned, suitably aligned buffer; the body is read
/// separately so the caller controls whether it is materialized or skipped.
class MessageReader
{
public:
    explicit MessageReader(ReadBuffer & in_) : in(in_) { }

    struct Message
    {
        /// Points into `metadata_storage`; valid until the next `readNextMessage` call.
        const flatbuf::Message * header = nullptr;
        int64_t body_length = 0;
    };

    /// Reads the metadata of the next message. Returns false on end-of-stream (the EOS marker or eof).
    bool readNextMessage(Message & out);

    /// Reads the message body (`body_length` bytes) into `body`, leaving the buffer positioned right after it.
    void readBody(int64_t body_length, PODArray<char> & body);

    /// Skips the message body without materializing it.
    void skipBody(int64_t body_length);

private:
    ReadBuffer & in;
    PODArray<char> metadata_storage;
};

}

#endif
