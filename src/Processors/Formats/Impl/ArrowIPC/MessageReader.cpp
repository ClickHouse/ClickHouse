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

bool MessageReader::readNextMessage(Message & out, int64_t max_metadata_length)
{
    if (in.eof())
        return false;

    int32_t metadata_length = readInt32LE(in);
    if (metadata_length == IPC_CONTINUATION_TOKEN)
    {
        /// Modern framing is the continuation token followed by the real metadata length; end of stream
        /// is the token followed by a zero length. The token alone (EOF right after it) is a truncated
        /// stream, so always read the next int32 and let `readStrict` report `CANNOT_READ_ALL_DATA` if it
        /// is missing, rather than accepting the truncated input as a clean end of stream.
        metadata_length = readInt32LE(in);
    }

    /// A zero length is the legacy end-of-stream marker.
    if (metadata_length == 0)
        return false;

    /// In the file format the footer's `Block.metaDataLength` is the authoritative size of this message's
    /// metadata section, so cap the metadata read by it (when given): otherwise a malformed footer could
    /// advertise a tiny block while the length prefix at its offset declares a much larger metadata message,
    /// driving an allocation of up to `MAX_REASONABLE_METADATA_LENGTH` before the body-length check runs.
    const int64_t metadata_cap
        = max_metadata_length < 0 ? MAX_REASONABLE_METADATA_LENGTH : std::min<int64_t>(max_metadata_length, MAX_REASONABLE_METADATA_LENGTH);
    if (metadata_length < 0 || metadata_length > metadata_cap)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not an Arrow IPC stream: implausible message metadata length {}", metadata_length);

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

void MessageReader::readBody(int64_t body_length, PODArray<char> & body)
{
    body.resize(body_length);
    if (body_length > 0)
        in.readStrict(body.data(), body_length);
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
