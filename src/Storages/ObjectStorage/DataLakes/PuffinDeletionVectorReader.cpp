#include <Storages/ObjectStorage/DataLakes/PuffinDeletionVectorReader.h>
#include <Core/Defines.h>

#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileSize.h>
#include <base/arithmeticOverflow.h>

#include <algorithm>
#include <cstring>

namespace ProfileEvents
{
extern const Event PuffinFilesRead;
extern const Event PuffinFileReadMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void validateDeletionVectorV1Fields(const std::vector<Int32> & fields, size_t blob_index)
{
    if (fields.empty())
        return;

    /// Spark / Iceberg file-scoped DVs use the reserved `_pos` id as a singleton marker.
    if (fields.size() == 1 && fields[0] == ICEBERG_ROW_POSITION_FIELD_ID)
        return;

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Puffin blob {}: deletion-vector-v1 has unsupported non-empty 'fields' "
        "(only [] or [{}] / Iceberg _pos are accepted; column-scoped DVs are not supported)",
        blob_index,
        ICEBERG_ROW_POSITION_FIELD_ID);
}

namespace
{

struct ScopedPuffinFileReadProfileEvent
{
    ProfileEventTimeIncrement<Microseconds> watch;

    ScopedPuffinFileReadProfileEvent()
        : watch(ProfileEvents::PuffinFileReadMicroseconds)
    {
        ProfileEvents::increment(ProfileEvents::PuffinFilesRead);
    }
};

UInt32 readBigEndianUInt32(const UInt8 * data)
{
    return (static_cast<UInt32>(data[0]) << 24)
        | (static_cast<UInt32>(data[1]) << 16)
        | (static_cast<UInt32>(data[2]) << 8)
        | static_cast<UInt32>(data[3]);
}

}

void validatePuffinBlobBounds(Int64 offset, Int64 length, size_t file_size, std::string_view context)
{
    if (offset < 0 || length < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: offset/length out of bounds", context);

    if (offset > static_cast<Int64>(file_size) || length > static_cast<Int64>(file_size))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: offset/length out of bounds", context);

    Int64 end = 0;
    if (common::addOverflow(offset, length, end))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: offset/length out of bounds", context);

    if (static_cast<UInt64>(end) > file_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: offset/length out of bounds", context);
}

void checkDeletionVectorBlobReadLimits(Int64 length, std::optional<UInt64> expected_cardinality)
{
    /// Same fail-closed order as the SQL `Puffin` path: cardinality before blob length / allocate.
    if (expected_cardinality.has_value() && *expected_cardinality > PUFFIN_DV_MAX_MATERIALIZED_POSITIONS)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Deletion vector cardinality {} exceeds materialization limit {}",
            *expected_cardinality,
            PUFFIN_DV_MAX_MATERIALIZED_POSITIONS);

    if (length < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector blob length is negative");

    if (static_cast<UInt64>(length) > PUFFIN_DV_MAX_BLOB_SIZE)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Deletion vector blob length {} exceeds absolute limit {}",
            length,
            PUFFIN_DV_MAX_BLOB_SIZE);

    if (static_cast<UInt64>(length) < 12)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector blob is too small");
}

void validateDeletionVectorEnvelope(const UInt8 * header, Int64 length)
{
    const UInt32 combined_length = readBigEndianUInt32(header);
    if (std::memcmp(header + sizeof(UInt32), DELETION_VECTOR_MAGIC, sizeof(DELETION_VECTOR_MAGIC)) != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid deletion vector magic");

    if (combined_length < sizeof(DELETION_VECTOR_MAGIC))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid deletion vector combined length: {}", combined_length);

    UInt64 expected_blob_size = 0;
    if (common::addOverflow(static_cast<UInt64>(combined_length), UInt64{8}, expected_blob_size))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid deletion vector combined length: {}", combined_length);

    if (static_cast<UInt64>(length) != expected_blob_size)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Deletion vector blob size {} does not match combined length {}",
            length,
            combined_length);
}

std::vector<UInt64> deserializeDeletionVectorV1Blob(std::string_view blob_bytes, std::optional<UInt64> expected_cardinality)
{
    if (expected_cardinality.has_value() && *expected_cardinality > PUFFIN_DV_MAX_MATERIALIZED_POSITIONS)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Deletion vector cardinality {} exceeds materialization limit {}",
            *expected_cardinality,
            PUFFIN_DV_MAX_MATERIALIZED_POSITIONS);

    return deserializeRoaringPositionBitmap(extractDeletionVectorPayload(blob_bytes), expected_cardinality);
}

std::vector<UInt64> readDeletionVectorFromPuffin(ReadBuffer & file, Int64 offset, Int64 length, std::optional<UInt64> expected_cardinality)
{
    ScopedPuffinFileReadProfileEvent profile_event;

    checkDeletionVectorBlobReadLimits(length, expected_cardinality);

    if (auto file_size = tryGetFileSizeFromReadBuffer(file))
        validatePuffinBlobBounds(offset, length, *file_size);
    else if (offset < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin deletion vector offset {} or length {}", offset, length);

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(&file);
    if (!seekable)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin deletion vector read requires a seekable buffer");

    /// Peek combined_length + magic before allocating `length` (up to 2 GiB). Matches
    /// `readDeletionVectorBlobBytes` in the SQL Puffin format path.
    seekable->seek(offset, SEEK_SET);

    UInt8 header[8];
    file.readStrict(reinterpret_cast<char *>(header), sizeof(header));
    validateDeletionVectorEnvelope(header, length);

    String blob_data(static_cast<size_t>(length), '\0');
    std::memcpy(blob_data.data(), header, sizeof(header));
    file.readStrict(blob_data.data() + sizeof(header), blob_data.size() - sizeof(header));

    return deserializeDeletionVectorV1Blob(blob_data, expected_cardinality);
}

void appendReadBufferWithAbsoluteSizeLimit(ReadBuffer & buf, std::vector<UInt8> & out, size_t max_buffered_size)
{
    if (out.size() > max_buffered_size)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin non-seekable buffer size {} exceeds absolute limit {}",
            out.size(),
            max_buffered_size);
    }

    std::vector<UInt8> tmp(DBMS_DEFAULT_BUFFER_SIZE);
    while (!buf.eof())
    {
        const size_t capacity = max_buffered_size - out.size();
        if (capacity == 0)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Puffin non-seekable input exceeds absolute buffer limit {} bytes; use seekable input for larger files",
                max_buffered_size);
        }

        const size_t to_read = std::min(tmp.size(), capacity);
        const size_t n = buf.read(reinterpret_cast<char *>(tmp.data()), to_read);
        if (n == 0)
            break;

        out.insert(out.end(), tmp.data(), tmp.data() + n);

        /// If we filled the remaining capacity and the stream still has data, fail closed.
        if (n == capacity && !buf.eof())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Puffin non-seekable input exceeds absolute buffer limit {} bytes; use seekable input for larger files",
                max_buffered_size);
        }
    }
}

const PuffinBlob & bindDeletionVectorBlob(
    const std::vector<PuffinBlob> & blobs,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    std::string_view expected_referenced_data_file,
    UInt64 expected_cardinality)
{
    const PuffinBlob * matched = nullptr;
    size_t matched_index = 0;

    for (size_t i = 0; i < blobs.size(); ++i)
    {
        if (blobs[i].offset != content_offset || blobs[i].length != content_size_in_bytes)
            continue;

        if (matched)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Multiple Puffin blobs claim offset {} length {}",
                content_offset,
                content_size_in_bytes);
        }

        matched = &blobs[i];
        matched_index = i;
    }

    if (!matched)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "No Puffin footer blob at offset {} length {}",
            content_offset,
            content_size_in_bytes);
    }

    if (matched->type != "deletion-vector-v1")
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin blob {} at offset {} length {} has type '{}', expected deletion-vector-v1",
            matched_index,
            content_offset,
            content_size_in_bytes,
            matched->type);
    }

    if (!matched->compression_codec.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin blob {}: deletion-vector-v1 must omit compression-codec",
            matched_index);
    }

    /// Structural DV footer checks live in the pre-existing SQL `Puffin` helper.
    const UInt64 footer_cardinality = requireDeletionVectorV1Properties(*matched, matched_index);

    const auto & referenced_data_file = matched->properties.at("referenced-data-file");
    if (referenced_data_file != expected_referenced_data_file)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin blob {} referenced-data-file '{}' does not match expected data file '{}'",
            matched_index,
            referenced_data_file,
            expected_referenced_data_file);
    }

    if (footer_cardinality != expected_cardinality)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin blob {} cardinality {} does not match expected cardinality {}",
            matched_index,
            footer_cardinality,
            expected_cardinality);
    }

    return *matched;
}

}
