#include <Processors/Formats/Impl/ArrowIPC/MessageReader.h>

#if USE_ARROW

#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/NetUtils.h>

#include <algorithm>
#include <cstdio>
#include <utility>

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
constexpr Int32 IPC_CONTINUATION_TOKEN = -1;

/// Even a schema with thousands of columns has a FlatBuffer well under a megabyte. Any larger
/// metadata length almost certainly means the input is not Arrow IPC (e.g. JSON misread as a size),
/// so we reject it before allocating, mirroring the guard in the Arrow-library based reader.
constexpr Int64 MAX_REASONABLE_METADATA_LENGTH = 256 * 1024 * 1024;

Int32 readInt32LE(ReadBuffer & in)
{
    Int32 value = 0;
    in.readStrict(reinterpret_cast<char *>(&value), sizeof(value));
    return DB::fromLittleEndian(value);
}

}

bool MessageReader::readNextMessage(Message & out, Int64 expected_metadata_length)
{
    if (in.eof())
        return false;

    /// Bytes consumed by the message framing before the metadata flatbuffer: a 4-byte length prefix, plus
    /// 4 more when the modern continuation token precedes it.
    Int64 framing = sizeof(Int32);
    Int32 metadata_length = readInt32LE(in);
    if (metadata_length == IPC_CONTINUATION_TOKEN)
    {
        /// Modern framing is the continuation token followed by the real metadata length; end of stream
        /// is the token followed by a zero length. The token alone (EOF right after it) is a truncated
        /// stream, so always read the next int32 and let `readStrict` report `CANNOT_READ_ALL_DATA` if it
        /// is missing, rather than accepting the truncated input as a clean end of stream.
        framing += sizeof(Int32);
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

    /// FlatBuffers verification only proves that a union value, *if present*, is a well-formed table; the
    /// `Message.header` union is not `(required)`, so `VerifyMessageBuffer` accepts a message whose
    /// `header_type` discriminant is set while the header value offset is absent (`Verifier::VerifyTable`
    /// returns true for a null table). Every reader checks `header_type()` and then dereferences the typed
    /// `header_as_X()` accessor, which returns null in exactly this case. Reject it here so a single guard
    /// covers all those call sites instead of each one dereferencing null.
    if (out.header->header_type() != flatbuf::MessageHeader_NONE && out.header->header() == nullptr)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC message header type is set but its value is missing");

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
Int64 referencedBodyLength(const flatbuf::RecordBatch & batch, Int64 body_length)
{
    const auto * buffers = batch.buffers();
    if (buffers == nullptr)
        return 0;

    Int64 used = 0;
    for (flatbuffers::uoffset_t i = 0; i < buffers->size(); ++i)
    {
        const auto * buffer = buffers->Get(i);
        const Int64 offset = buffer->offset();
        const Int64 length = buffer->length();
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

void MessageReader::readBody(
    const flatbuf::RecordBatch & batch, Int64 body_length, PODArray<char> & body, const VectorWithMemoryTracking<char> * reachable)
{
    if (reachable == nullptr)
    {
        const Int64 used = referencedBodyLength(batch, body_length);
        body.resize(used);
        if (used > 0)
            in.readStrict(body.data(), used);
        /// Consume the trailing bytes the buffers do not reference (inter-buffer/end padding, or a declared body
        /// longer than the buffers need) so the stream is positioned at the next message. `skipBody` reads
        /// through the buffer without allocating, so an over-long declared tail fails cleanly as
        /// CANNOT_READ_ALL_DATA instead of triggering a large allocation here.
        skipBody(body_length - used);
        return;
    }

    /// Subset read: validate and collect only the buffers the requested columns reference, then read just
    /// those ranges into their absolute offsets, skipping the gaps left by unrequested columns. Unreachable
    /// buffers are neither validated nor read, so a corrupt unrequested column cannot fail the read. `body`
    /// is still sized to the maximum reachable `offset + length` (sparse fill, gaps left unwritten); the
    /// decoder indexes it by absolute offset. Compacting and remapping offsets would also shrink the
    /// allocation and is a possible future improvement.
    const auto * buffers = batch.buffers();
    const size_t num_buffers = buffers ? buffers->size() : 0;
    VectorWithMemoryTracking<std::pair<Int64, Int64>> ranges;
    Int64 used = 0;
    for (size_t i = 0; i < num_buffers; ++i)
    {
        if (i >= reachable->size() || !(*reachable)[i])
            continue;
        const auto * buffer = buffers->Get(static_cast<flatbuffers::uoffset_t>(i));
        const Int64 offset = buffer->offset();
        const Int64 length = buffer->length();
        /// An empty buffer may carry a placeholder offset (e.g. -1); it references no body bytes.
        if (length == 0)
            continue;
        if (offset < 0 || length < 0 || length > body_length || offset > body_length - length)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Arrow IPC buffer (offset {}, length {}) is out of the message body of size {}",
                offset, length, body_length);
        ranges.emplace_back(offset, length);
        used = std::max(used, offset + length);
    }

    body.resize(used);
    std::sort(ranges.begin(), ranges.end());

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(&in);
    auto skip = [&](Int64 n)
    {
        if (n <= 0)
            return;
        /// On a seekable input the gap bytes are never read; on a stream they are read and discarded (the
        /// same short-read-safe path as `skipBody`).
        if (seekable)
            seekable->seek(seekable->getPosition() + n, SEEK_SET);
        else if (in.tryIgnore(n) != static_cast<size_t>(n))
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all data from the Arrow IPC message body");
    };

    Int64 cur = 0;
    for (const auto & [offset, length] : ranges)
    {
        const Int64 end = offset + length;
        if (end <= cur)
            continue; /// already covered by a previous (overlapping) range
        const Int64 start = std::max(offset, cur);
        if (start > cur)
            skip(start - cur);
        in.readStrict(body.data() + start, end - start);
        cur = end;
    }
    /// Skip the present body bytes after the last reachable buffer: unrequested columns whose ranges are
    /// in-bounds. These exist, so they are `seek`-skipped (never read) and deliberately not validated — a
    /// corrupt or truncated *unrequested* column must not fail a subset read. An out-of-bounds (corrupt)
    /// unrequested buffer is ignored here rather than read.
    Int64 referenced_end = cur;
    for (size_t i = 0; i < num_buffers; ++i)
    {
        const auto * buffer = buffers->Get(static_cast<flatbuffers::uoffset_t>(i));
        const Int64 offset = buffer->offset();
        const Int64 length = buffer->length();
        if (length > 0 && offset >= 0 && length <= body_length && offset <= body_length - length)
            referenced_end = std::max(referenced_end, offset + length);
    }
    if (referenced_end > cur)
    {
        skip(referenced_end - cur);
        cur = referenced_end;
    }

    /// The declared tail that no buffer references must still be present: validate it with a read-through
    /// (`skipBody`) rather than a `seek`, so a forged-huge `Message.bodyLength` whose tail is absent fails as
    /// CANNOT_READ_ALL_DATA instead of being silently `seek`-skipped past end of file.
    skipBody(body_length - cur);
}

void MessageReader::skipBody(Int64 body_length)
{
    /// Report a truncated body the same way `readBody` (via `readStrict`) does — as `CANNOT_READ_ALL_DATA`,
    /// not the bare `ATTEMPT_TO_READ_AFTER_EOF` that `ignore` would throw — so the error code does not depend
    /// on whether the count-only optimisation skipped the body or actually read it.
    if (body_length > 0 && in.tryIgnore(body_length) != static_cast<size_t>(body_length))
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all data from the Arrow IPC message body");
}

}

#endif
