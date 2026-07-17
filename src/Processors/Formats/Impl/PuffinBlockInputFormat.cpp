#include <limits>
#include <optional>
#include <unordered_map>
#include <vector>
#include <zlib.h>
#include <lz4frame.h>
#include <roaring/roaring.hh>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileSize.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Core/Defines.h>
#include <IO/ReadBuffer.h>
#include <base/arithmeticOverflow.h>
#include <base/types.h>
#include <base/unaligned.h>
#include <Processors/Formats/Impl/PuffinBlockInputFormat.h>
#include <IO/ReadBufferFromMemory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LZ4_DECODER_FAILED;
}

namespace
{

constexpr UInt8 PUFFIN_MAGIC[4] = {0x50, 0x46, 0x41, 0x31};
constexpr UInt8 PUFFIN_FOOTER_COMPRESSED_FLAG = 0x01;
constexpr size_t PUFFIN_FOOTER_TRAILER_SIZE = 12;
constexpr size_t PUFFIN_FOOTER_LZ4_MAX_RATIO = 255;
/// Absolute cap on materialized deleted positions (~800 MiB of UInt64s at this limit).
constexpr UInt64 PUFFIN_DV_MAX_MATERIALIZED_POSITIONS = 100'000'000;
constexpr UInt8 DELETION_VECTOR_MAGIC[4] = {0xD1, 0xD3, 0x39, 0x64};
/// Matches Iceberg `RoaringPositionBitmap.MAX_POSITION` / iceberg-cpp `kMaxPosition`.
/// Key INT32_MAX is unsupported: the reference bitmap array length is a signed Int32.
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

void checkMagic(const UInt8 * p, const char * context)
{
    if (std::memcmp(p, PUFFIN_MAGIC, 4) != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin magic ({})", context);
}

/// `blob_region_end` is the end of the `Blob₁ ... Blobₙ` region, i.e. the offset of the footer's
/// leading magic. Blob offset/length must stay within [PUFFIN_MAGIC, blob_region_end), so a malformed
/// footer cannot point a blob into the footer payload or trailer.
void validatePuffinBlobBounds(Int64 offset, Int64 length, size_t blob_region_end, size_t blob_index)
{
    if (offset < static_cast<Int64>(sizeof(PUFFIN_MAGIC)) || length < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: offset/length out of bounds", blob_index);

    Int64 end = 0;
    if (common::addOverflow(offset, length, end))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: offset/length out of bounds", blob_index);

    if (static_cast<UInt64>(end) > blob_region_end)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: offset/length out of bounds", blob_index);
}

void validatePuffinFooterFlags(const UInt8 flags[4])
{
    if (flags[1] != 0 || flags[2] != 0 || flags[3] != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Puffin footer flags");
    if ((flags[0] & ~PUFFIN_FOOTER_COMPRESSED_FLAG) != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Puffin footer flags");
}

String decompressPuffinFooterPayload(const char * data, size_t size)
{
    LZ4F_dctx * dctx = nullptr;
    size_t ret = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
    if (LZ4F_isError(ret))
        throw Exception(ErrorCodes::LZ4_DECODER_FAILED, "Failed to create LZ4 decompression context: {}", LZ4F_getErrorName(ret));

    struct DecompressionContextGuard
    {
        LZ4F_dctx * dctx;
        explicit DecompressionContextGuard(LZ4F_dctx * dctx_) : dctx(dctx_) { }
        ~DecompressionContextGuard() { LZ4F_freeDecompressionContext(dctx); }
    };
    DecompressionContextGuard guard(dctx);

    LZ4F_frameInfo_t frame_info{};
    size_t header_size = size;
    ret = LZ4F_getFrameInfo(dctx, &frame_info, data, &header_size);
    if (LZ4F_isError(ret))
        throw Exception(ErrorCodes::LZ4_DECODER_FAILED, "Failed to read LZ4 frame info for Puffin footer: {}", LZ4F_getErrorName(ret));

    const char * src = data + header_size;
    size_t src_remaining = size - header_size;

    size_t max_decompressed_size = 0;
    if (common::mulOverflow(size, PUFFIN_FOOTER_LZ4_MAX_RATIO, max_decompressed_size))
        throw Exception(ErrorCodes::LZ4_DECODER_FAILED, "Puffin footer compressed payload is too large");

    if (frame_info.contentSize == 0)
        throw Exception(ErrorCodes::LZ4_DECODER_FAILED, "Puffin footer LZ4 frame must declare content size");

    if (frame_info.contentSize > max_decompressed_size)
        throw Exception(
            ErrorCodes::LZ4_DECODER_FAILED,
            "Puffin footer LZ4 content size {} exceeds decompression limit {}",
            frame_info.contentSize,
            max_decompressed_size);

    String result;
    result.resize(static_cast<size_t>(frame_info.contentSize));

    size_t dst_offset = 0;
    while (true)
    {
        size_t src_read = src_remaining;
        size_t dst_write = result.size() - dst_offset;
        ret = LZ4F_decompress(dctx, result.data() + dst_offset, &dst_write, src, &src_read, nullptr);
        if (LZ4F_isError(ret))
            throw Exception(ErrorCodes::LZ4_DECODER_FAILED, "Failed to decompress Puffin footer: {}", LZ4F_getErrorName(ret));

        src += src_read;
        src_remaining -= src_read;
        dst_offset += dst_write;

        if (ret == 0)
            break;

        /// No forward progress while the frame is still open means the input is truncated:
        /// LZ4F_decompress wants more input but we have given it everything. Stop to avoid spinning.
        if (dst_write == 0 && src_read == 0)
            throw Exception(ErrorCodes::LZ4_DECODER_FAILED, "Puffin footer LZ4 frame is incomplete");

        if (dst_offset == result.size())
            throw Exception(
                ErrorCodes::LZ4_DECODER_FAILED,
                "Puffin footer decompressed size exceeds declared content size {}",
                frame_info.contentSize);
    }

    if (dst_offset != frame_info.contentSize)
        throw Exception(
            ErrorCodes::LZ4_DECODER_FAILED,
            "Puffin footer LZ4 decompressed size {} does not match content size {}",
            dst_offset,
            frame_info.contentSize);

    if (src_remaining != 0)
        throw Exception(
            ErrorCodes::LZ4_DECODER_FAILED,
            "Puffin footer LZ4 frame has {} trailing bytes",
            src_remaining);

    result.resize(dst_offset);
    return result;
}

void requireBlobMetadataField(const Poco::JSON::Object::Ptr & blob_obj, const char * field_name, size_t blob_index)
{
    if (!blob_obj->has(field_name) || blob_obj->isNull(field_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: missing required field '{}'", blob_index, field_name);
}

/// Poco stores JSON integers that do not fit signed Int64 as unsigned; convert<Int64>() would throw RangeException.
std::optional<Int64> tryJSONIntegerAsInt64(const Poco::Dynamic::Var & value)
{
    if (value.isInteger() && !value.isSigned())
    {
        const auto as_uint64 = value.convert<UInt64>();
        if (as_uint64 > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            return std::nullopt;
        return static_cast<Int64>(as_uint64);
    }

    return value.convert<Int64>();
}

Int64 requireBlobMetadataInt64(const Poco::JSON::Object::Ptr & blob_obj, const char * field_name, size_t blob_index)
{
    requireBlobMetadataField(blob_obj, field_name, blob_index);
    const Poco::Dynamic::Var & value = blob_obj->get(field_name);
    if (!value.isInteger())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: field '{}' must be an integer", blob_index, field_name);

    auto as_int64 = tryJSONIntegerAsInt64(value);
    if (!as_int64)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: field '{}' is out of Int64 range", blob_index, field_name);
    return *as_int64;
}

Int32 requireBlobMetadataFieldsElementInt32(const Poco::Dynamic::Var & value, size_t blob_index, size_t field_index)
{
    if (!value.isInteger())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: fields[{}] must be an integer", blob_index, field_index);

    auto as_int64 = tryJSONIntegerAsInt64(value);
    if (!as_int64)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: fields[{}] is out of Int64 range", blob_index, field_index);

    if (*as_int64 < std::numeric_limits<Int32>::min() || *as_int64 > std::numeric_limits<Int32>::max())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: fields[{}] is out of Int32 range", blob_index, field_index);

    return static_cast<Int32>(*as_int64);
}

String requireJSONStringValue(const Poco::Dynamic::Var & value, size_t blob_index, const char * field_name)
{
    if (!value.isString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: field '{}' must be a string", blob_index, field_name);
    return value.extract<String>();
}

String requireBlobMetadataString(const Poco::JSON::Object::Ptr & blob_obj, const char * field_name, size_t blob_index)
{
    requireBlobMetadataField(blob_obj, field_name, blob_index);
    return requireJSONStringValue(blob_obj->get(field_name), blob_index, field_name);
}

String optBlobMetadataString(const Poco::JSON::Object::Ptr & blob_obj, const char * field_name, size_t blob_index)
{
    if (!blob_obj->has(field_name) || blob_obj->isNull(field_name))
        return {};
    return requireJSONStringValue(blob_obj->get(field_name), blob_index, field_name);
}

void requireDeletionVectorV1Properties(const PuffinBlob & blob, size_t blob_index)
{
    static constexpr const char * required_properties[] = {"referenced-data-file", "cardinality"};
    for (const char * key : required_properties)
    {
        auto it = blob.properties.find(key);
        if (it == blob.properties.end() || it->second.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Puffin blob {}: deletion-vector-v1 missing required property '{}'",
                blob_index,
                key);
    }
}

void parseBlobProperties(const Poco::JSON::Object::Ptr & blob_obj, PuffinBlob & blob, size_t blob_index, bool required)
{
    if (!blob_obj->has("properties") || blob_obj->isNull("properties"))
    {
        if (required)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: missing required field 'properties'", blob_index);
        return;
    }

    auto props_obj = blob_obj->getObject("properties");
    if (!props_obj)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: field 'properties' must be an object", blob_index);

    for (const auto & [key, val] : *props_obj)
    {
        if (!val.isString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: property '{}' must be a string", blob_index, key);
        blob.properties.emplace(key, val.extract<String>());
    }
}

std::vector<PuffinBlob> parseFooterJSON(const String & footer_json, size_t blob_region_end)
{
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var root;
    try
    {
        root = parser.parse(footer_json);
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Puffin footer JSON: {}", e.displayText());
    }

    if (root.type() != typeid(Poco::JSON::Object::Ptr))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin footer JSON must be an object");

    auto obj = root.extract<Poco::JSON::Object::Ptr>();

    if (!obj->has("blobs") || obj->isNull("blobs"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin footer is missing required field 'blobs'");

    auto blobs_arr = obj->getArray("blobs");
    if (!blobs_arr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin footer field 'blobs' must be an array");

    std::vector<PuffinBlob> blobs;
    for (size_t i = 0; i < blobs_arr->size(); ++i)
    {
        auto blob_obj = blobs_arr->getObject(static_cast<unsigned>(i));
        if (!blob_obj)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: must be an object", i);

        PuffinBlob blob;

        blob.type = requireBlobMetadataString(blob_obj, "type", i);
        blob.snapshot_id = requireBlobMetadataInt64(blob_obj, "snapshot-id", i);
        blob.sequence_number = requireBlobMetadataInt64(blob_obj, "sequence-number", i);
        blob.offset = requireBlobMetadataInt64(blob_obj, "offset", i);
        blob.length = requireBlobMetadataInt64(blob_obj, "length", i);
        blob.compression_codec = optBlobMetadataString(blob_obj, "compression-codec", i);

        if (blob.type == "deletion-vector-v1")
        {
            if (blob.snapshot_id != -1 || blob.sequence_number != -1)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Puffin blob {}: deletion-vector-v1 snapshot-id and sequence-number must be -1",
                    i);

            if (blob_obj->has("compression-codec") && !blob_obj->isNull("compression-codec"))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Puffin blob {}: deletion-vector-v1 must omit 'compression-codec'",
                    i);

            parseBlobProperties(blob_obj, blob, i, /*required=*/true);
            requireDeletionVectorV1Properties(blob, i);
        }
        else
            parseBlobProperties(blob_obj, blob, i, /*required=*/false);

        requireBlobMetadataField(blob_obj, "fields", i);
        auto fields_arr = blob_obj->getArray("fields");
        if (!fields_arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: field 'fields' must be an array", i);

        for (size_t j = 0; j < fields_arr->size(); ++j)
            blob.fields.push_back(requireBlobMetadataFieldsElementInt32(fields_arr->get(static_cast<unsigned>(j)), i, j));

        validatePuffinBlobBounds(blob.offset, blob.length, blob_region_end, i);

        blobs.push_back(std::move(blob));
    }
    return blobs;
}

std::vector<PuffinBlob> readPuffinFooterFromSeekable(SeekableReadBuffer & seekable, size_t file_size)
{
    if (file_size < 16)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin file too small");

    seekable.seek(0, SEEK_SET);
    char magic_buf[4];
    seekable.readStrict(magic_buf, 4);
    checkMagic(reinterpret_cast<const UInt8 *>(magic_buf), "header");

    seekable.seek(static_cast<off_t>(file_size - PUFFIN_FOOTER_TRAILER_SIZE), SEEK_SET);
    Int32 footer_length_signed = 0;
    readBinaryLittleEndian(footer_length_signed, seekable);

    UInt8 flags[4] = {};
    seekable.readStrict(reinterpret_cast<char *>(flags), sizeof(flags));

    char trailing_buf[4];
    seekable.readStrict(trailing_buf, 4);
    checkMagic(reinterpret_cast<const UInt8 *>(trailing_buf), "trailing");
    validatePuffinFooterFlags(flags);

    if (footer_length_signed <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin footer length: {}", footer_length_signed);

    UInt64 footer_trailer_size = 0;
    if (common::addOverflow(static_cast<UInt64>(footer_length_signed), PUFFIN_FOOTER_TRAILER_SIZE, footer_trailer_size)
        || footer_trailer_size > file_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin footer length: {}", footer_length_signed);

    const size_t footer_length = static_cast<size_t>(footer_length_signed);
    const size_t payload_start = file_size - PUFFIN_FOOTER_TRAILER_SIZE - footer_length;
    if (payload_start < sizeof(PUFFIN_MAGIC))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin footer length: {}", footer_length_signed);

    seekable.seek(static_cast<off_t>(payload_start - sizeof(PUFFIN_MAGIC)), SEEK_SET);
    char leading_magic_buf[4];
    seekable.readStrict(leading_magic_buf, 4);
    checkMagic(reinterpret_cast<const UInt8 *>(leading_magic_buf), "footer");

    String footer_payload(footer_length, '\0');
    seekable.seek(static_cast<off_t>(payload_start), SEEK_SET);
    seekable.readStrict(footer_payload.data(), footer_length);

    String footer_json;
    if ((flags[0] & PUFFIN_FOOTER_COMPRESSED_FLAG) != 0)
        footer_json = decompressPuffinFooterPayload(footer_payload.data(), footer_payload.size());
    else
        footer_json = std::move(footer_payload);

    const size_t blob_region_end = payload_start - sizeof(PUFFIN_MAGIC);
    return parseFooterJSON(footer_json, blob_region_end);
}

PuffinFooter readPuffinFooter(ReadBuffer & buf, bool seekable_read)
{
    PuffinFooter result;

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(&buf);
    auto file_size_opt = tryGetFileSizeFromReadBuffer(buf);

    /// Pipes/FIFOs are SeekableReadBuffer subclasses but fstat reports size 0; require a real
    /// regular file before trusting seek+size (same pattern as ORC/Arrow). Also honor
    /// `input_format_allow_seeks` via FormatSettings.seekable_read.
    if (seekable_read && seekable && seekable->checkIfActuallySeekable() && file_size_opt)
    {
        result.blobs = readPuffinFooterFromSeekable(*seekable, *file_size_opt);
    }
    else
    {
        std::vector<UInt8> tmp(DEFAULT_BLOCK_SIZE);
        while (!buf.eof())
        {
            size_t n = buf.read(reinterpret_cast<char *>(tmp.data()), tmp.size());
            result.data.insert(result.data.end(), tmp.data(), tmp.data() + n);
        }

        ReadBufferFromMemory mem_buf(result.data.data(), result.data.size());
        result.blobs = readPuffinFooterFromSeekable(mem_buf, result.data.size());
    }

    return result;
}

String readPuffinBlobBytes(
    const PuffinBlob & blob, ReadBuffer & buf, const std::vector<UInt8> & data, bool seekable_read)
{
    const size_t length = static_cast<size_t>(blob.length);

    /// When the footer path buffered the whole file, copy from that buffer — the original input
    /// may still look Seekable (e.g. a consumed pipe) but must not be seeked.
    if (!data.empty())
    {
        if (static_cast<UInt64>(blob.offset) + static_cast<UInt64>(blob.length) > data.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob offset/length out of bounds of buffered data");

        return String(reinterpret_cast<const char *>(data.data() + blob.offset), length);
    }

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(&buf);
    if (!seekable_read || !seekable || !seekable->checkIfActuallySeekable())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read Puffin blob: input is not seekable and was not buffered");

    seekable->seek(blob.offset, SEEK_SET);
    String result(length, '\0');
    seekable->readStrict(result.data(), length);
    return result;
}

roaring::Roaring readRoaringPortableSafe(const char * data, size_t size, Int32 key)
{
    try
    {
        return roaring::Roaring::readSafe(data, size);
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to deserialize deletion vector roaring bitmap at key {}: {}", key, e.what());
    }
}

void deserializeRoaringPositionBitmap(std::string_view bytes, UInt64 expected_cardinality, ColumnUInt64 & positions)
{
    if (bytes.size() < sizeof(Int64))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector bitmap is too small");

    const char * ptr = bytes.data();
    size_t remaining = bytes.size();

    /// Iceberg deletion-vector roaring layout stores count and keys as little-endian.
    const Int64 bitmap_count = unalignedLoadLittleEndian<Int64>(ptr);
    ptr += sizeof(Int64);
    remaining -= sizeof(Int64);

    if (bitmap_count < 0 || bitmap_count > std::numeric_limits<Int32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid deletion vector bitmap count: {}", bitmap_count);

    Int32 last_key = -1;
    Int32 remaining_count = static_cast<Int32>(bitmap_count);
    UInt64 running_cardinality = 0;

    while (remaining_count > 0)
    {
        if (remaining < sizeof(Int32))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector bitmap is truncated while reading key");

        const Int32 key = unalignedLoadLittleEndian<Int32>(ptr);
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

        const UInt64 bitmap_cardinality = bitmap.cardinality();
        UInt64 new_running_cardinality = 0;
        if (common::addOverflow(running_cardinality, bitmap_cardinality, new_running_cardinality))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Deletion vector cardinality exceeds declared cardinality {}",
                expected_cardinality);

        if (new_running_cardinality > expected_cardinality)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Deletion vector cardinality {} exceeds declared cardinality {}",
                new_running_cardinality,
                expected_cardinality);

        running_cardinality = new_running_cardinality;

        for (UInt32 sub_position : bitmap)
        {
            const UInt64 position = positionFromKeyAndSubPosition(static_cast<UInt32>(key), sub_position);
            if (position > static_cast<UInt64>(DELETION_VECTOR_MAX_POSITION))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector position {} is out of supported range", position);
            positions.insertValue(position);
        }

        ptr += bitmap_size;
        remaining -= bitmap_size;
        last_key = key;
        --remaining_count;
    }

    if (remaining != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector bitmap has {} trailing bytes", remaining);

    if (running_cardinality != expected_cardinality)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Deletion vector cardinality {} does not match deserialized row count {}",
            expected_cardinality,
            running_cardinality);
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

void deserializeDeletionVectorV1(std::string_view blob, UInt64 expected_cardinality, ColumnUInt64 & positions)
{
    if (expected_cardinality > PUFFIN_DV_MAX_MATERIALIZED_POSITIONS)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Deletion vector cardinality {} exceeds materialization limit {}",
            expected_cardinality,
            PUFFIN_DV_MAX_MATERIALIZED_POSITIONS);

    deserializeRoaringPositionBitmap(extractDeletionVectorPayload(blob), expected_cardinality, positions);
}

NamesAndTypesList getPuffinMetadataSchema()
{
    return {
        {"blob_type", std::make_shared<DataTypeString>()},
        {"snapshot_id", std::make_shared<DataTypeInt64>()},
        {"sequence_number", std::make_shared<DataTypeInt64>()},
        {"fields", std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>())},
        {"offset", std::make_shared<DataTypeInt64>()},
        {"length", std::make_shared<DataTypeInt64>()},
        {"compression_codec", std::make_shared<DataTypeString>()},
        {"properties", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
    };
}

NamesAndTypesList getPuffinSchema()
{
    return {
        {"referenced_data_file", std::make_shared<DataTypeString>()},
        {"deleted_rows", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
    };
}

void checkPuffinFormatHeader(const Block & header, const NamesAndTypesList & expected_schema, const char * format_name)
{
    std::unordered_map<String, DataTypePtr> name_to_type;
    for (const auto & [name, type] : expected_schema)
        name_to_type[name] = type;

    String allowed_columns;
    for (const auto & [name, type] : expected_schema)
    {
        if (!allowed_columns.empty())
            allowed_columns += ", ";
        allowed_columns += name;
    }

    for (const auto & [name, type] : header.getNamesAndTypes())
    {
        auto it = name_to_type.find(name);
        if (it == name_to_type.end())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected column: {}. {} format allows only the next columns: {}",
                name,
                format_name,
                allowed_columns);

        if (!it->second->equals(*type))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected type {} for column {}. Expected type: {}",
                type->getName(),
                name,
                it->second->getName());
    }
}

void checkPuffinMetadataHeader(const Block & header)
{
    checkPuffinFormatHeader(header, getPuffinMetadataSchema(), "PuffinMetadata");
}

void checkPuffinHeader(const Block & header)
{
    checkPuffinFormatHeader(header, getPuffinSchema(), "Puffin");
}

}

PuffinMetadataInputFormat::PuffinMetadataInputFormat(ReadBuffer & buf, SharedHeader header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &buf)
    , seekable_read(format_settings_.seekable_read)
{
    checkPuffinMetadataHeader(getPort().getHeader());
}

Chunk PuffinMetadataInputFormat::read()
{
    if (!initialized)
    {
        blob_index = 0;
        footer = readPuffinFooter(*in, seekable_read);
        initialized = true;
    }
    if (footer.blobs.size() <= blob_index)
        return {};

    const PuffinBlob & blob = footer.blobs[blob_index++];

    auto col_type = ColumnString::create();
    auto col_snap = ColumnInt64::create();
    auto col_seq = ColumnInt64::create();
    auto col_fields_data = ColumnInt32::create();
    auto col_fields_offsets = ColumnArray::ColumnOffsets::create();
    auto col_offset = ColumnInt64::create();
    auto col_length = ColumnInt64::create();
    auto col_codec = ColumnString::create();
    auto col_props_keys = ColumnString::create();
    auto col_props_vals = ColumnString::create();
    auto col_props_offsets = ColumnArray::ColumnOffsets::create();

    col_type->insertData(blob.type.data(), blob.type.size());
    col_snap->insertValue(blob.snapshot_id);
    col_seq->insertValue(blob.sequence_number);
    for (Int32 f : blob.fields)
        col_fields_data->insertValue(f);
    col_fields_offsets->insertValue(blob.fields.size());
    col_offset->insertValue(blob.offset);
    col_length->insertValue(blob.length);
    col_codec->insertData(blob.compression_codec.data(), blob.compression_codec.size());
    for (const auto & [k, v] : blob.properties)
    {
        col_props_keys->insertData(k.data(), k.size());
        col_props_vals->insertData(v.data(), v.size());
    }
    col_props_offsets->insertValue(blob.properties.size());

    auto col_fields = ColumnArray::create(std::move(col_fields_data), std::move(col_fields_offsets));
    MutableColumns prop_cols;
    prop_cols.push_back(std::move(col_props_keys));
    prop_cols.push_back(std::move(col_props_vals));
    MutableColumnPtr col_props_tuple = ColumnTuple::create(std::move(prop_cols));
    MutableColumnPtr col_props_arr = ColumnArray::create(std::move(col_props_tuple), std::move(col_props_offsets));
    MutableColumnPtr col_props = ColumnMap::create(std::move(col_props_arr));

    std::unordered_map<String, MutableColumnPtr> built;
    built.emplace("blob_type",         std::move(col_type));
    built.emplace("snapshot_id",       std::move(col_snap));
    built.emplace("sequence_number",   std::move(col_seq));
    built.emplace("fields",            std::move(col_fields));
    built.emplace("offset",            std::move(col_offset));
    built.emplace("length",            std::move(col_length));
    built.emplace("compression_codec", std::move(col_codec));
    built.emplace("properties",        std::move(col_props));

    const Block & out_header = getPort().getHeader();
    MutableColumns result;
    result.reserve(out_header.columns());
    for (const auto & col_with_name : out_header)
        result.push_back(std::move(built.at(col_with_name.name)));
    return Chunk(std::move(result), 1);
}

PuffinInputFormat::PuffinInputFormat(ReadBuffer & buf, SharedHeader header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &buf)
    , seekable_read(format_settings_.seekable_read)
{
    checkPuffinHeader(getPort().getHeader());
}

Chunk PuffinInputFormat::read()
{
    if (!initialized)
    {
        blob_index = 0;
        footer = readPuffinFooter(*in, seekable_read);
        initialized = true;
    }

    while (blob_index < footer.blobs.size())
    {
        const size_t current_blob_index = blob_index;
        const auto & blob = footer.blobs[blob_index++];

        if (blob.type != "deletion-vector-v1")
            continue;

        UInt64 expected_cardinality = 0;
        if (!tryParse(expected_cardinality, blob.properties.at("cardinality")))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Puffin blob {}: deletion-vector-v1 property 'cardinality' must be an unsigned integer",
                current_blob_index);

        const auto & referenced_data_file = blob.properties.at("referenced-data-file");

        const String blob_data = readPuffinBlobBytes(blob, *in, footer.data, seekable_read);
        auto col_rows_data = ColumnUInt64::create();
        deserializeDeletionVectorV1(blob_data, expected_cardinality, *col_rows_data);

        auto col_file = ColumnString::create();
        col_file->insertData(referenced_data_file.data(), referenced_data_file.size());

        auto col_rows_offsets = ColumnArray::ColumnOffsets::create();
        col_rows_offsets->insertValue(col_rows_data->size());
        auto col_rows = ColumnArray::create(std::move(col_rows_data), std::move(col_rows_offsets));

        const Block & out_header = getPort().getHeader();
        MutableColumns result;
        result.reserve(out_header.columns());
        for (const auto & col_with_name : out_header)
        {
            if (col_with_name.name == "referenced_data_file")
                result.push_back(std::move(col_file));
            else
                result.push_back(std::move(col_rows));
        }
        return Chunk(std::move(result), 1);
    }

    return {};
}

void PuffinMetadataInputFormat::resetParser()
{
    IInputFormat::resetParser();
    initialized = false;
    blob_index = 0;
    footer = {};
}

void PuffinInputFormat::resetParser()
{
    IInputFormat::resetParser();
    initialized = false;
    blob_index = 0;
    footer = {};
}

PuffinMetadataSchemaReader::PuffinMetadataSchemaReader(ReadBuffer & in_)
    : ISchemaReader(in_)
{
}

NamesAndTypesList PuffinMetadataSchemaReader::readSchema()
{
    return getPuffinMetadataSchema();
}

PuffinSchemaReader::PuffinSchemaReader(ReadBuffer & in_)
    : ISchemaReader(in_)
{
}

NamesAndTypesList PuffinSchemaReader::readSchema()
{
    return getPuffinSchema();
}

void registerInputFormatPuffin(FormatFactory & factory)
{
    factory.registerInputFormat(
        "PuffinMetadata",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams &, const FormatSettings & settings)
        { return std::make_shared<PuffinMetadataInputFormat>(buf, std::make_shared<const Block>(sample), settings); });
    factory.markFormatSupportsSubsetOfColumns("PuffinMetadata");

    factory.setDocumentation("PuffinMetadata", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

Special input format for reading [Apache Iceberg Puffin](https://iceberg.apache.org/puffin-spec/) file footer metadata.
It outputs one row per blob entry from the footer `BlobMetadata` list.

Fixed output columns:
- `blob_type` (`String`) - blob type, for example `deletion-vector-v1`
- `snapshot_id` (`Int64`) - snapshot id of the blob
- `sequence_number` (`Int64`) - sequence number of the blob
- `fields` (`Array(Int32)`) - sorted list of field ids the blob applies to
- `offset` (`Int64`) - offset of the blob payload in the file
- `length` (`Int64`) - length of the blob payload in bytes
- `compression_codec` (`String`) - compression codec of the blob payload, if present
- `properties` (`Map(String, String)`) - blob-specific properties

LZ4-compressed puffin footers are supported.

## Example usage {#example-usage}

Inspect footer blobs:

```sql
SELECT blob_type, snapshot_id, sequence_number, offset, length, compression_codec,
       mapKeys(properties), mapValues(properties)
FROM file(deletes.puffin, PuffinMetadata);
```

Pair with the `Puffin` format to read `deletion-vector-v1` blob payloads.
)DOCS_MD",
        .related = {"Puffin"},
    });

    factory.registerInputFormat(
        "Puffin",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams &, const FormatSettings & settings)
        { return std::make_shared<PuffinInputFormat>(buf, std::make_shared<const Block>(sample), settings); });
    factory.markFormatSupportsSubsetOfColumns("Puffin");

    factory.setDocumentation("Puffin", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

Input format for reading [Apache Iceberg Puffin](https://iceberg.apache.org/puffin-spec/) files.

The format exposes deleted row positions from `deletion-vector-v1` blobs. Other blob types (for example `apache-datasketches-theta-v1`) are skipped.
If a puffin file contains multiple `deletion-vector-v1` blobs, the format outputs one row per such blob.

Fixed output columns:
- `referenced_data_file` (`String`) - location of the data file the deletion vector applies to (`referenced-data-file` blob property)
- `deleted_rows` (`Array(UInt64)`) - 64-bit row positions deleted according to the deletion vector roaring bitmap

Deletion vectors whose declared `cardinality` exceeds an absolute materialization ceiling are rejected.

Only a subset of output columns can be requested. A user-provided structure with unexpected column names or types is rejected when the format is created.

## Example usage {#example-usage}

Read deleted row positions with the referenced data file:

```sql
SELECT referenced_data_file, deleted_rows
FROM file(deletes.puffin, Puffin);
```

Expand deleted positions into individual rows:

```sql
SELECT referenced_data_file, row_number
FROM file(deletes.puffin, Puffin)
ARRAY JOIN deleted_rows AS row_number
ORDER BY referenced_data_file, row_number;
```

Use `PuffinMetadata` to inspect footer blob descriptors before reading deletion vectors.
)DOCS_MD",
        .related = {"PuffinMetadata"},
    });
}

void registerPuffinSchemaReaders(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "PuffinMetadata",
        [](ReadBuffer & buf, const FormatSettings &)
        { return std::make_shared<PuffinMetadataSchemaReader>(buf); });

    factory.registerSchemaReader(
        "Puffin",
        [](ReadBuffer & buf, const FormatSettings &)
        { return std::make_shared<PuffinSchemaReader>(buf); });
}

}
