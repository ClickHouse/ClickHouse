#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeletionVector.h>

#if USE_DELTA_KERNEL_RS

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/Puffin/RoaringPositionBitmap.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <base/unaligned.h>
#include <Common/Exception.h>

#include <array>
#include <zlib.h>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int NOT_IMPLEMENTED;
}

namespace DeltaLake
{

namespace
{

constexpr UInt32 PORTABLE_ROARING_BITMAP_MAGIC = 1681511377;
constexpr UInt32 MAX_DELETION_VECTOR_SIZE = 1U << 31;
constexpr size_t ENCODED_UUID_LENGTH = 20;
constexpr std::string_view Z85_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

constexpr std::array<Int8, 256> makeZ85DecodeTable()
{
    std::array<Int8, 256> table{};
    for (auto & value : table)
        value = -1;
    for (size_t i = 0; i < Z85_ALPHABET.size(); ++i)
        table[static_cast<UInt8>(Z85_ALPHABET[i])] = static_cast<Int8>(i);
    return table;
}

constexpr auto Z85_DECODE_TABLE = makeZ85DecodeTable();

std::string formatUUID(std::string_view bytes)
{
    static constexpr std::string_view hex_digits = "0123456789abcdef";
    std::string result;
    result.reserve(36);
    for (size_t i = 0; i < bytes.size(); ++i)
    {
        if (i == 4 || i == 6 || i == 8 || i == 10)
            result.push_back('-');
        const auto byte = static_cast<UInt8>(bytes[i]);
        result.push_back(hex_digits[byte >> 4]);
        result.push_back(hex_digits[byte & 0xF]);
    }
    return result;
}

std::string_view getPortableBitmap(std::string_view serialized, const DeletionVectorDescriptor & descriptor)
{
    if (serialized.size() < sizeof(UInt32))
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Deletion vector `{}` is too small", descriptor.path_or_inline_dv);

    const auto magic = unalignedLoadLittleEndian<UInt32>(serialized.data());
    if (magic != PORTABLE_ROARING_BITMAP_MAGIC)
        throw DB::Exception(
            DB::ErrorCodes::CORRUPTED_DATA, "Deletion vector `{}` has an unsupported magic number {}", descriptor.path_or_inline_dv, magic);

    return serialized.substr(sizeof(UInt32));
}

std::string readSerializedDeletionVector(
    const DeletionVectorDescriptor & descriptor,
    const IKernelHelper & helper,
    const DB::ObjectStoragePtr & object_storage,
    const DB::ContextPtr & context)
{
    const auto & path = descriptor.storage_type == 'u' ? getDeletionVectorRelativePath(descriptor) : descriptor.path_or_inline_dv;
    const auto object_path = helper.resolveDataFilePath(path, context);
    auto buf = object_storage->readObject(DB::StoredObject(object_path), context->getReadSettings());

    UInt8 format_version = 0;
    DB::readBinary(format_version, *buf);
    if (format_version != 1)
        throw DB::Exception(
            DB::ErrorCodes::CORRUPTED_DATA, "Deletion vector file `{}` has an unsupported format version {}", object_path, static_cast<UInt32>(format_version));

    const Int32 start = descriptor.offset.value_or(1);
    if (start < 1)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Deletion vector in `{}` has an invalid offset {}", object_path, start);
    buf->seek(start, SEEK_SET);

    UInt32 size = 0;
    DB::readBinaryBigEndian(size, *buf);
    if (size != static_cast<UInt32>(descriptor.size_in_bytes) || size > MAX_DELETION_VECTOR_SIZE)
        throw DB::Exception(
            DB::ErrorCodes::CORRUPTED_DATA,
            "Deletion vector in `{}` at offset {} has size {}, while the log records {}",
            object_path, start, size, descriptor.size_in_bytes);

    std::string serialized(size, '\0');
    buf->readStrict(serialized.data(), size);

    UInt32 expected_checksum = 0;
    DB::readBinaryBigEndian(expected_checksum, *buf);
    const auto checksum = static_cast<UInt32>(crc32_z(0L, reinterpret_cast<const unsigned char *>(serialized.data()), serialized.size()));
    if (checksum != expected_checksum)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Deletion vector in `{}` at offset {} has a wrong checksum", object_path, start);

    return serialized;
}

}

std::string decodeZ85(std::string_view encoded)
{
    if (encoded.size() % 5 != 0)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Z85 string `{}` has a length that is not a multiple of 5", encoded);

    std::string decoded;
    decoded.reserve(encoded.size() / 5 * 4);
    for (size_t i = 0; i < encoded.size(); i += 5)
    {
        UInt64 value = 0;
        for (size_t j = i; j < i + 5; ++j)
        {
            const auto digit = Z85_DECODE_TABLE[static_cast<UInt8>(encoded[j])];
            if (digit < 0)
                throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Z85 string `{}` has an invalid character", encoded);
            value = value * 85 + static_cast<UInt64>(digit);
        }
        if (value > std::numeric_limits<UInt32>::max())
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Z85 string `{}` has an invalid group", encoded);
        for (int shift = 24; shift >= 0; shift -= 8)
            decoded.push_back(static_cast<char>((value >> shift) & 0xFF));
    }
    return decoded;
}

std::string getDeletionVectorRelativePath(const DeletionVectorDescriptor & descriptor)
{
    const auto & encoded = descriptor.path_or_inline_dv;
    if (encoded.size() < ENCODED_UUID_LENGTH)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Deletion vector path `{}` is too short", encoded);

    const auto prefix = std::string_view(encoded).substr(0, encoded.size() - ENCODED_UUID_LENGTH);
    const auto file_name = "deletion_vector_" + formatUUID(decodeZ85(std::string_view(encoded).substr(prefix.size()))) + ".bin";
    if (prefix.empty())
        return file_name;
    return std::string(prefix) + "/" + file_name;
}

DB::DataLakeObjectMetadata::ExcludedRowsPtr readDeletionVector(
    const DeletionVectorDescriptor & descriptor,
    const IKernelHelper & helper,
    const DB::ObjectStoragePtr & object_storage,
    const DB::ContextPtr & context)
{
    if (descriptor.size_in_bytes < static_cast<Int32>(sizeof(UInt32)) || descriptor.cardinality < 0)
        throw DB::Exception(
            DB::ErrorCodes::CORRUPTED_DATA,
            "Deletion vector `{}` has an invalid size {} or cardinality {}",
            descriptor.path_or_inline_dv, descriptor.size_in_bytes, descriptor.cardinality);

    std::string serialized;
    switch (descriptor.storage_type)
    {
        case 'i':
        {
            serialized = decodeZ85(descriptor.path_or_inline_dv);
            if (serialized.size() < static_cast<size_t>(descriptor.size_in_bytes))
                throw DB::Exception(
                    DB::ErrorCodes::CORRUPTED_DATA,
                    "Inline deletion vector has {} bytes, while the log records {}",
                    serialized.size(), descriptor.size_in_bytes);
            serialized.resize(descriptor.size_in_bytes);
            break;
        }
        case 'u':
        case 'p':
            serialized = readSerializedDeletionVector(descriptor, helper, object_storage, context);
            break;
        default:
            throw DB::Exception(
                DB::ErrorCodes::NOT_IMPLEMENTED, "Deletion vector storage type `{}` is not supported", descriptor.storage_type);
    }

    auto excluded_rows = std::make_shared<DB::DataLakeObjectMetadata::ExcludedRows>();
    DB::deserializeRoaringPositionBitmap(
        getPortableBitmap(serialized, descriptor),
        static_cast<UInt64>(descriptor.cardinality),
        [&](UInt64 position) { excluded_rows->add(position); });
    return excluded_rows;
}

}

#endif
