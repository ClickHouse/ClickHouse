#include <Storages/ObjectStorage/DataLakes/PuffinDeletionVectorReader.h>

#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileSize.h>
#include <base/arithmeticOverflow.h>

#include <roaring/roaring.hh>

#include <cstring>
#include <limits>
#include <zlib.h>

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
    extern const int INCORRECT_DATA;
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

constexpr UInt8 DELETION_VECTOR_MAGIC[4] = {0xD1, 0xD3, 0x39, 0x64};
constexpr Int64 DELETION_VECTOR_MAX_POSITION = 0x7FFFFFFE80000000LL;
constexpr Int32 DELETION_VECTOR_MAX_KEY = std::numeric_limits<Int32>::max() - 1;

UInt32 readBigEndianUInt32(const UInt8 * data)
{
    return (static_cast<UInt32>(data[0]) << 24)
        | (static_cast<UInt32>(data[1]) << 16)
        | (static_cast<UInt32>(data[2]) << 8)
        | static_cast<UInt32>(data[3]);
}

UInt64 positionFromKeyAndSubPosition(UInt32 key, UInt32 sub_position)
{
    return (static_cast<UInt64>(key) << 32) | static_cast<UInt64>(sub_position);
}

roaring::Roaring readRoaringPortableSafe(const char * data, size_t size, Int32 key)
{
    try
    {
        return roaring::Roaring::readSafe(data, size);
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to deserialize deletion vector roaring bitmap at key {}: {}", key, e.what());
    }
}

std::vector<UInt64> deserializeRoaringPositionBitmap(std::string_view bytes)
{
    if (bytes.size() < sizeof(Int64))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector bitmap is too small");

    const char * ptr = bytes.data();
    size_t remaining = bytes.size();

    Int64 bitmap_count = 0;
    std::memcpy(&bitmap_count, ptr, sizeof(Int64));
    ptr += sizeof(Int64);
    remaining -= sizeof(Int64);

    if (bitmap_count < 0 || bitmap_count > std::numeric_limits<Int32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid deletion vector bitmap count: {}", bitmap_count);

    std::vector<UInt64> positions;
    Int32 last_key = -1;
    Int32 remaining_count = static_cast<Int32>(bitmap_count);

    while (remaining_count > 0)
    {
        if (remaining < sizeof(Int32))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector bitmap is truncated while reading key");

        Int32 key = 0;
        std::memcpy(&key, ptr, sizeof(Int32));
        ptr += sizeof(Int32);
        remaining -= sizeof(Int32);

        if (key < 0 || key > DELETION_VECTOR_MAX_KEY)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid deletion vector bitmap key: {}", key);
        if (key <= last_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector bitmap keys must be sorted in ascending order");

        auto bitmap = readRoaringPortableSafe(ptr, remaining, key);

        const size_t bitmap_size = bitmap.getSizeInBytes(/*portable=*/true);
        if (bitmap_size > remaining)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector roaring bitmap at key {} exceeds blob size", key);

        for (UInt32 sub_position : bitmap)
        {
            const UInt64 position = positionFromKeyAndSubPosition(static_cast<UInt32>(key), sub_position);
            if (position > static_cast<UInt64>(DELETION_VECTOR_MAX_POSITION))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector position {} is out of supported range", position);
            positions.push_back(position);
        }

        ptr += bitmap_size;
        remaining -= bitmap_size;
        last_key = key;
        --remaining_count;
    }

    if (remaining != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector bitmap has {} trailing bytes", remaining);

    return positions;
}

std::string_view extractDeletionVectorPayload(std::string_view blob)
{
    if (blob.size() < 12)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector blob is too small");

    const auto * blob_bytes = reinterpret_cast<const UInt8 *>(blob.data());
    const UInt32 combined_length = readBigEndianUInt32(blob_bytes);
    if (combined_length < sizeof(DELETION_VECTOR_MAGIC))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid deletion vector combined length: {}", combined_length);

    const size_t vector_size = combined_length - sizeof(DELETION_VECTOR_MAGIC);
    const size_t expected_blob_size = sizeof(UInt32) + combined_length + sizeof(UInt32);
    if (blob.size() != expected_blob_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector blob size {} does not match combined length {}", blob.size(), combined_length);

    if (std::memcmp(blob_bytes + sizeof(UInt32), DELETION_VECTOR_MAGIC, sizeof(DELETION_VECTOR_MAGIC)) != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid deletion vector magic");

    const UInt8 * crc_input = blob_bytes + sizeof(UInt32);
    const size_t crc_input_size = combined_length;
    const UInt32 expected_crc = readBigEndianUInt32(blob_bytes + sizeof(UInt32) + combined_length);
    const UInt32 actual_crc = static_cast<UInt32>(crc32_z(0L, reinterpret_cast<const unsigned char *>(crc_input), crc_input_size));
    if (expected_crc != actual_crc)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector CRC mismatch");

    return std::string_view(blob.data() + 2 * sizeof(UInt32), vector_size);
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

std::vector<UInt64> deserializeDeletionVectorV1Blob(std::string_view blob_bytes)
{
    const std::string_view vector_bytes = extractDeletionVectorPayload(blob_bytes);
    return deserializeRoaringPositionBitmap(vector_bytes);
}

std::vector<UInt64> readDeletionVectorFromPuffin(ReadBuffer & file, Int64 offset, Int64 length)
{
    ScopedPuffinFileReadProfileEvent profile_event;

    if (auto file_size = tryGetFileSizeFromReadBuffer(file))
        validatePuffinBlobBounds(offset, length, *file_size);
    else if (offset < 0 || length < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin deletion vector offset {} or length {}", offset, length);

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(&file);
    if (!seekable)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin deletion vector read requires a seekable buffer");

    seekable->seek(offset, SEEK_SET);

    String blob_data(static_cast<size_t>(length), '\0');
    file.readStrict(blob_data.data(), blob_data.size());

    return deserializeDeletionVectorV1Blob(blob_data);
}

}
