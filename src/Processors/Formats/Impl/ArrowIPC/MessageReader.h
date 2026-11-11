#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>

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
        Int64 body_length = 0;
    };

    /// Reads the metadata of the next message. Returns false on end-of-stream (the EOS marker or eof).
    /// `expected_metadata_length`, when non-negative, is the exact size of the whole metadata section
    /// (framing + padded flatbuffer): pass the footer `Block.metaDataLength` in the file format so a
    /// mismatch is rejected before allocating and the following body is read from the footer-declared
    /// boundary. The stream format leaves it at the default (only the absolute `MAX_REASONABLE` ceiling).
    bool readNextMessage(Message & out, Int64 expected_metadata_length = -1);

    /// Reads the message body of a record/dictionary batch into `body`, leaving the buffer positioned right
    /// after the whole `body_length`-byte body (so the next message follows). Only the prefix actually
    /// referenced by `batch.buffers()` is materialized: the maximum `buffer.offset + buffer.length` is
    /// validated against the untrusted `body_length` and used as the allocation size, and any trailing
    /// padding or over-long declared tail is skipped rather than allocated. This stops a forged-huge
    /// `Message.bodyLength` paired with tiny buffers from driving a large allocation before the buffer
    /// ranges are validated.
    ///
    /// `reachable`, when set, is a 0/1 mask over `batch.buffers()` (see `RecordBatchDecoder::reachable
    /// TopLevelBuffers`). Only the buffers it marks are validated and read — into their absolute offsets in
    /// `body`, with the gaps left by unrequested columns skipped (a `seek` on a seekable input, an `ignore`
    /// on a stream). `body` is still sized to the maximum reachable `offset + length`, so a requested column
    /// late in the layout keeps the buffer large; compacting the body and remapping buffer offsets to shrink
    /// the allocation is a possible future improvement.
    void readBody(const flatbuf::RecordBatch & batch, Int64 body_length, PODArray<char> & body,
                  const VectorWithMemoryTracking<char> * reachable = nullptr);

    /// Skips the message body without materializing it.
    void skipBody(Int64 body_length);

private:
    ReadBuffer & in;
    PODArray<char> metadata_storage;
};

}

#endif
