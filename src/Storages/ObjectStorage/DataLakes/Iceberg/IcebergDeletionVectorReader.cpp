#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDeletionVectorReader.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Common/Exception.h>

#include <roaring/roaring64map.hh>
#include <zlib.h>

#include <cstring>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
}

namespace DB::Iceberg
{
namespace
{
UInt32 loadBigEndianUInt32(const char * data)
{
    return (static_cast<UInt32>(static_cast<unsigned char>(data[0])) << 24)
        | (static_cast<UInt32>(static_cast<unsigned char>(data[1])) << 16)
        | (static_cast<UInt32>(static_cast<unsigned char>(data[2])) << 8)
        | static_cast<UInt32>(static_cast<unsigned char>(data[3]));
}

}

std::unique_ptr<roaring::Roaring64Map> readIcebergDeletionVector(
    const String & file_path,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    UInt64 max_content_size_in_bytes,
    const ObjectStoragePtr & object_storage,
    ContextPtr context,
    LoggerPtr log)
{
    if (content_offset < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg deletion vector content offset cannot be negative: {}", content_offset);
    if (content_size_in_bytes < 12)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Iceberg deletion vector blob is too small: {}", content_size_in_bytes);
    if (max_content_size_in_bytes && static_cast<UInt64>(content_size_in_bytes) > max_content_size_in_bytes)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Iceberg deletion vector blob is too large: {} bytes, maximum: {} bytes",
            content_size_in_bytes,
            max_content_size_in_bytes);

    RelativePathWithMetadata object_info(file_path);
    auto read_buffer = createReadBuffer(object_info, object_storage, context, log);
    read_buffer->seek(content_offset, SEEK_SET);

    String blob(static_cast<size_t>(content_size_in_bytes), '\0');
    read_buffer->readStrict(blob.data(), blob.size());

    const UInt32 total_length = loadBigEndianUInt32(blob.data());
    if (total_length + 8 != blob.size())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Iceberg deletion vector length mismatch, expected: {}, actual: {}",
            total_length + 8,
            blob.size());

    static constexpr char magic[] = {'\xD1', '\xD3', '\x39', '\x64'};
    if (memcmp(blob.data() + sizeof(total_length), magic, sizeof(magic)) != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Iceberg deletion vector magic number mismatch");

    const UInt32 expected_crc = loadBigEndianUInt32(blob.data() + blob.size() - 4);
    const UInt32 actual_crc = static_cast<UInt32>(crc32(
        0,
        reinterpret_cast<const Bytef *>(blob.data() + sizeof(total_length)),
        static_cast<uInt>(blob.size() - sizeof(total_length) - sizeof(expected_crc))));
    if (expected_crc != actual_crc)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Iceberg deletion vector CRC mismatch, expected: {}, actual: {}",
            expected_crc,
            actual_crc);

    try
    {
        return std::make_unique<roaring::Roaring64Map>(roaring::Roaring64Map::readSafe(blob.data() + 8, blob.size() - 12));
    }
    catch (const std::runtime_error & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot decode Iceberg deletion vector bitmap: {}", e.what());
    }
}

}
