#include <Storages/ObjectStorage/DataLakes/PuffinFile.h>

#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <IO/ReadHelpers.h>
#include <base/arithmeticOverflow.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <lz4frame.h>

#include <cstring>
#include <limits>

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
    extern const int LZ4_DECODER_FAILED;
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

constexpr UInt8 PUFFIN_MAGIC[4] = {0x50, 0x46, 0x41, 0x31};
constexpr UInt8 PUFFIN_FOOTER_COMPRESSED_FLAG = 0x01;
constexpr size_t PUFFIN_FOOTER_TRAILER_SIZE = 12;
constexpr size_t PUFFIN_FOOTER_LZ4_MAX_RATIO = 255;
/// Absolute cap on footer payload size (uncompressed JSON bytes, or declared LZ4 contentSize).
/// Prevents crafted FooterPayloadSize / contentSize values from forcing huge allocations.
/// Intentionally a compile-time safety ceiling (same class as other format hard caps), not a
/// FormatSettings / input_format_puffin_* knob: oversized footers are never legitimate input.
constexpr size_t PUFFIN_FOOTER_MAX_PAYLOAD_SIZE = 16 * 1024 * 1024;
/// DV blob / materialization ceilings live in `PuffinDeletionVectorReader.h` (`PUFFIN_DV_MAX_*`)
/// and are shared with the Iceberg deletion-vector reader. Applied only when `deleted_rows` is
/// requested; subset reads that skip materialization do not enforce the materialization ceiling.
/// Intentionally not FormatSettings knobs: fail-closed amplification guards.

void checkMagic(const UInt8 * p, const char * context)
{
    if (std::memcmp(p, PUFFIN_MAGIC, 4) != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin magic ({})", context);
}

/// `blob_region_end` is the end of the `Blob₁ ... Blobₙ` region, i.e. the offset of the footer's
/// leading magic. Blob offset/length must stay within [PUFFIN_MAGIC, blob_region_end), so a malformed
/// footer cannot point a blob into the footer payload or trailer.
void validatePuffinFooterBlobBounds(Int64 offset, Int64 length, size_t blob_region_end, size_t blob_index)
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

    size_t max_by_ratio = 0;
    if (common::mulOverflow(size, PUFFIN_FOOTER_LZ4_MAX_RATIO, max_by_ratio))
        throw Exception(ErrorCodes::LZ4_DECODER_FAILED, "Puffin footer compressed payload is too large");

    if (frame_info.contentSize == 0)
        throw Exception(ErrorCodes::LZ4_DECODER_FAILED, "Puffin footer LZ4 frame must declare content size");

    const size_t max_decompressed_size = std::min(max_by_ratio, PUFFIN_FOOTER_MAX_PAYLOAD_SIZE);
    if (frame_info.contentSize > max_decompressed_size)
    {
        if (max_decompressed_size == PUFFIN_FOOTER_MAX_PAYLOAD_SIZE)
            throw Exception(
                ErrorCodes::LZ4_DECODER_FAILED,
                "Puffin footer LZ4 content size {} exceeds absolute decompression limit {}",
                frame_info.contentSize,
                PUFFIN_FOOTER_MAX_PAYLOAD_SIZE);

        throw Exception(
            ErrorCodes::LZ4_DECODER_FAILED,
            "Puffin footer LZ4 content size {} exceeds decompression limit {}",
            frame_info.contentSize,
            max_decompressed_size);
    }

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

/// Poco reports JSON booleans as integers (`std::numeric_limits<bool>::is_integer`).
bool isJSONInteger(const Poco::Dynamic::Var & value)
{
    return value.isInteger() && !value.isBoolean();
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
    if (!isJSONInteger(value))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: field '{}' must be an integer", blob_index, field_name);

    auto as_int64 = tryJSONIntegerAsInt64(value);
    if (!as_int64)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: field '{}' is out of Int64 range", blob_index, field_name);
    return *as_int64;
}

Int32 requireBlobMetadataFieldsElementInt32(const Poco::Dynamic::Var & value, size_t blob_index, size_t field_index)
{
    if (!isJSONInteger(value))
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
    if (!blob_obj->has(field_name))
        return {};
    if (blob_obj->isNull(field_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: field '{}' must be a string", blob_index, field_name);
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

    UInt64 cardinality = 0;
    if (!tryParse(cardinality, blob.properties.at("cardinality")))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin blob {}: deletion-vector-v1 property 'cardinality' must be an unsigned integer",
            blob_index);
}

void parseStringValuedProperties(
    const Poco::JSON::Object::Ptr & props_obj,
    std::map<String, String> * out,
    bool for_blob,
    size_t blob_index)
{
    for (const auto & [key, val] : *props_obj)
    {
        if (!val.isString())
        {
            if (for_blob)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: property '{}' must be a string", blob_index, key);
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin footer property '{}' must be a string", key);
        }
        if (out)
            out->emplace(key, val.extract<String>());
    }
}

void parseBlobProperties(const Poco::JSON::Object::Ptr & blob_obj, PuffinBlob & blob, size_t blob_index, bool required)
{
    if (!blob_obj->has("properties"))
    {
        if (required)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: missing required field 'properties'", blob_index);
        return;
    }

    if (blob_obj->isNull("properties"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: field 'properties' must be an object", blob_index);

    auto props_obj = blob_obj->getObject("properties");
    if (!props_obj)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: field 'properties' must be an object", blob_index);

    parseStringValuedProperties(props_obj, &blob.properties, /*for_blob=*/true, blob_index);
}

/// Optional FileMetadata.properties: JSON object with string values (Iceberg Puffin spec).
/// Absent is allowed; present null / non-object is rejected.
void validateFileMetadataProperties(const Poco::JSON::Object::Ptr & footer_obj)
{
    if (!footer_obj->has("properties"))
        return;

    if (footer_obj->isNull("properties"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin footer field 'properties' must be an object");

    auto props_obj = footer_obj->getObject("properties");
    if (!props_obj)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin footer field 'properties' must be an object");

    parseStringValuedProperties(props_obj, /*out=*/nullptr, /*for_blob=*/false, /*blob_index=*/0);
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

    validateFileMetadataProperties(obj);

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

        if (blob.type == "deletion-vector-v1")
        {
            if (blob.snapshot_id != -1 || blob.sequence_number != -1)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Puffin blob {}: deletion-vector-v1 snapshot-id and sequence-number must be -1",
                    i);

            /// Spec requires the key to be omitted; present null is not omission.
            if (blob_obj->has("compression-codec"))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Puffin blob {}: deletion-vector-v1 must omit 'compression-codec'",
                    i);

            parseBlobProperties(blob_obj, blob, i, /*required=*/true);
            requireDeletionVectorV1Properties(blob, i);
        }
        else
        {
            blob.compression_codec = optBlobMetadataString(blob_obj, "compression-codec", i);
            parseBlobProperties(blob_obj, blob, i, /*required=*/false);
        }

        requireBlobMetadataField(blob_obj, "fields", i);
        auto fields_arr = blob_obj->getArray("fields");
        if (!fields_arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: field 'fields' must be an array", i);

        for (size_t j = 0; j < fields_arr->size(); ++j)
            blob.fields.push_back(requireBlobMetadataFieldsElementInt32(fields_arr->get(static_cast<unsigned>(j)), i, j));

        validatePuffinFooterBlobBounds(blob.offset, blob.length, blob_region_end, i);

        blobs.push_back(std::move(blob));
    }
    return blobs;
}

} // namespace

std::vector<PuffinBlob> readPuffinFooterBlobsFromSeekable(SeekableReadBuffer & seekable, size_t file_size)
{
    ScopedPuffinFileReadProfileEvent profile_event;

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
    if (common::addOverflow(
            static_cast<UInt64>(footer_length_signed),
            static_cast<UInt64>(PUFFIN_FOOTER_TRAILER_SIZE),
            footer_trailer_size)
        || footer_trailer_size > file_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin footer length: {}", footer_length_signed);

    const size_t footer_length = static_cast<size_t>(footer_length_signed);
    const size_t payload_start = file_size - PUFFIN_FOOTER_TRAILER_SIZE - footer_length;
    /// Footer layout is Magic | FooterPayload | …; require a distinct footer-open Magic after the
    /// file header Magic (payload_start == 4 would reuse the header Magic when blobs is empty).
    if (payload_start < 2 * sizeof(PUFFIN_MAGIC))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin footer length: {}", footer_length_signed);

    if (footer_length > PUFFIN_FOOTER_MAX_PAYLOAD_SIZE)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin footer payload size {} exceeds absolute limit {}",
            footer_length,
            PUFFIN_FOOTER_MAX_PAYLOAD_SIZE);

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

    const auto ref_it = matched->properties.find("referenced-data-file");
    if (ref_it == matched->properties.end() || ref_it->second.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin blob {}: deletion-vector-v1 missing required property 'referenced-data-file'",
            matched_index);
    }

    if (ref_it->second != expected_referenced_data_file)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin blob {} referenced-data-file '{}' does not match expected data file '{}'",
            matched_index,
            ref_it->second,
            expected_referenced_data_file);
    }

    const auto card_it = matched->properties.find("cardinality");
    if (card_it == matched->properties.end() || card_it->second.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin blob {}: deletion-vector-v1 missing required property 'cardinality'",
            matched_index);
    }

    UInt64 footer_cardinality = 0;
    if (!tryParse(footer_cardinality, card_it->second))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin blob {}: deletion-vector-v1 property 'cardinality' must be an unsigned integer",
            matched_index);
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
