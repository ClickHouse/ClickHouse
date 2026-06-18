#include <Processors/Formats/Impl/ArrowIPC/MessageReader.h>

#if USE_ARROW

#include <IO/ReadBuffer.h>
#include <IO/NetUtils.h>

#include <algorithm>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
}
}

namespace DB::ArrowIPC
{

namespace
{

/// 0xFFFFFFFF marks the modern (>= v0.15.0) encapsulated-message framing.
constexpr int32_t IPC_CONTINUATION_TOKEN = -1;

/// Even a schema with thousands of columns has a FlatBuffer well under a megabyte. Any larger
/// metadata length almost certainly means the input is not Arrow IPC (e.g. JSON misread as a size),
/// so we reject it before allocating, mirroring the guard in the Arrow-library based reader.
constexpr int64_t MAX_REASONABLE_METADATA_LENGTH = 256 * 1024 * 1024;

int32_t readInt32LE(ReadBuffer & in)
{
    int32_t value = 0;
    in.readStrict(reinterpret_cast<char *>(&value), sizeof(value));
    return DB::fromLittleEndian(value);
}

}

bool MessageReader::readNextMessage(Message & out, int64_t expected_metadata_length)
{
    if (in.eof())
        return false;

    /// Bytes consumed by the message framing before the metadata flatbuffer: a 4-byte length prefix, plus
    /// 4 more when the modern continuation token precedes it.
    int64_t framing = sizeof(int32_t);
    int32_t metadata_length = readInt32LE(in);
    if (metadata_length == IPC_CONTINUATION_TOKEN)
    {
        /// Modern framing is the continuation token followed by the real metadata length; end of stream
        /// is the token followed by a zero length. The token alone (EOF right after it) is a truncated
        /// stream, so always read the next int32 and let `readStrict` report `CANNOT_READ_ALL_DATA` if it
        /// is missing, rather than accepting the truncated input as a clean end of stream.
        framing += sizeof(int32_t);
        metadata_length = readInt32LE(in);
    }

    /// A zero length is the legacy end-of-stream marker.
    if (metadata_length == 0)
        return false;

    if (metadata_length < 0 || metadata_length > MAX_REASONABLE_METADATA_LENGTH)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not an Arrow IPC stream: implausible message metadata length {}", metadata_length);

    /// In the file format the caller passes the footer's `Block.metaDataLength` — the authoritative size of
    /// this message's whole metadata section (framing + padded flatbuffer). Enforce it exactly, before
    /// allocating: a malformed footer advertising a tiny block cannot then drive a large allocation from a
    /// forged length prefix, and a block whose on-wire metadata is smaller than the footer claims is
    /// rejected rather than leaving the body to be read from the wrong boundary.
    if (expected_metadata_length >= 0 && framing + metadata_length != expected_metadata_length)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC message metadata section is {} bytes but its footer block declares {}",
            framing + metadata_length, expected_metadata_length);

    metadata_storage.resize(metadata_length);
    in.readStrict(metadata_storage.data(), metadata_length);

    flatbuffers::Verifier verifier(reinterpret_cast<const uint8_t *>(metadata_storage.data()), metadata_length);
    if (!flatbuf::VerifyMessageBuffer(verifier))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted Arrow IPC message metadata");

    out.header = flatbuf::GetMessage(metadata_storage.data());
    out.body_length = out.header->bodyLength();
    if (out.body_length < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Negative Arrow IPC message body length {}", out.body_length);

    return true;
}

namespace
{

/// The number of body bytes actually referenced by the batch: the maximum `offset + length` over its
/// non-empty buffers. `Message.bodyLength` is untrusted and may be far larger (a forged value, or merely
/// the writer's trailing padding), so the body is read up to this bound instead of the declared length.
/// Each buffer is validated against `body_length` with overflow-safe arithmetic (`offset + length` cannot
/// wrap because `length <= body_length` is checked first), so a span pointing past the body is rejected
/// here rather than read.
int64_t referencedBodyLength(const flatbuf::RecordBatch & batch, int64_t body_length)
{
    const auto * buffers = batch.buffers();
    if (buffers == nullptr)
        return 0;

    int64_t used = 0;
    for (flatbuffers::uoffset_t i = 0; i < buffers->size(); ++i)
    {
        const auto * buffer = buffers->Get(i);
        const int64_t offset = buffer->offset();
        const int64_t length = buffer->length();
        /// An empty buffer may carry a placeholder offset (e.g. -1); it references no body bytes.
        if (length == 0)
            continue;
        if (offset < 0 || length < 0 || length > body_length || offset > body_length - length)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow IPC buffer (offset {}, length {}) is out of the message body of size {}",
                offset, length, body_length);
        used = std::max(used, offset + length);
    }
    return used;
}

}

void MessageReader::readBody(const flatbuf::RecordBatch & batch, int64_t body_length, PODArray<char> & body)
{
    const int64_t used = referencedBodyLength(batch, body_length);
    body.resize(used);
    if (used > 0)
        in.readStrict(body.data(), used);
    /// Consume the trailing bytes the buffers do not reference (inter-buffer/end padding, or a declared body
    /// longer than the buffers need) so the stream is positioned at the next message. `skipBody` reads
    /// through the buffer without allocating, so an over-long declared tail fails cleanly as
    /// CANNOT_READ_ALL_DATA instead of triggering a large allocation here.
    skipBody(body_length - used);
}

void MessageReader::skipBody(int64_t body_length)
{
    /// Report a truncated body the same way `readBody` (via `readStrict`) does — as `CANNOT_READ_ALL_DATA`,
    /// not the bare `ATTEMPT_TO_READ_AFTER_EOF` that `ignore` would throw — so the error code does not depend
    /// on whether the count-only optimisation skipped the body or actually read it.
    if (body_length > 0 && in.tryIgnore(body_length) != static_cast<size_t>(body_length))
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all data from the Arrow IPC message body");
}

}

#endif
