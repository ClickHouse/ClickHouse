#include <cstring>
#include <limits>
#include <unordered_map>
#include <vector>
#include <zlib.h>
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
#include <base/types.h>
#include <Processors/Formats/Impl/PuffinBlockInputFormat.h>
#include <IO/ReadBufferFromMemory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

constexpr UInt8 PUFFIN_MAGIC[4] = {0x50, 0x46, 0x41, 0x31};
constexpr UInt8 DELETION_VECTOR_MAGIC[4] = {0xD1, 0xD3, 0x39, 0x64};
constexpr Int64 DELETION_VECTOR_MAX_POSITION = 0x7FFFFFFE80000000LL;

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

std::vector<PuffinBlob> parseFooterJSON(const String & footer_json, size_t data_size)
{
    Poco::JSON::Parser parser;
    auto root = parser.parse(footer_json);
    const auto & obj = root.extract<Poco::JSON::Object::Ptr>();
    auto blobs_arr = obj->getArray("blobs");

    std::vector<PuffinBlob> blobs;
    for (size_t i = 0; i < blobs_arr->size(); ++i)
    {
        auto blob_obj = blobs_arr->getObject(static_cast<unsigned>(i));
        PuffinBlob blob;
        blob.type = blob_obj->getValue<String>("type");
        blob.snapshot_id = blob_obj->optValue<Int64>("snapshot-id", 0);
        blob.sequence_number = blob_obj->optValue<Int64>("sequence-number", 0);
        blob.offset = blob_obj->getValue<Int64>("offset");
        blob.length = blob_obj->getValue<Int64>("length");
        blob.compression_codec = blob_obj->optValue<String>("compression-codec", "");

        if (auto props_obj = blob_obj->getObject("properties"))
            for (const auto & [key, val] : *props_obj)
                blob.properties.emplace(key, val.extract<String>());

        if (auto fields_arr = blob_obj->getArray("fields"))
        {
            for (size_t j = 0; j < fields_arr->size(); ++j)
                blob.fields.push_back(fields_arr->getElement<Int32>(static_cast<unsigned>(j)));
        }

        if (blob.offset < 0 || blob.length < 0
            || static_cast<size_t>(blob.offset + blob.length) > data_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin blob {}: offset/length out of bounds", i);

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

    seekable.seek(static_cast<off_t>(file_size - 12), SEEK_SET);
    Int32 footer_length_signed = 0;
    readBinaryLittleEndian(footer_length_signed, seekable);

    seekable.seek(static_cast<off_t>(file_size - 4), SEEK_SET);
    char trailing_buf[4];
    seekable.readStrict(trailing_buf, 4);
    checkMagic(reinterpret_cast<const UInt8 *>(trailing_buf), "trailing");

    if (footer_length_signed <= 0
        || static_cast<size_t>(footer_length_signed) + 12 > file_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin footer length: {}", footer_length_signed);

    const size_t footer_length = static_cast<size_t>(footer_length_signed);
    String footer_json(footer_length, '\0');
    seekable.seek(static_cast<off_t>(file_size - 12 - footer_length), SEEK_SET);
    seekable.readStrict(footer_json.data(), footer_length);

    return parseFooterJSON(footer_json, file_size);
}

PuffinFooter readPuffinFooter(ReadBuffer & buf)
{
    PuffinFooter result;

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(&buf);
    auto file_size_opt = tryGetFileSizeFromReadBuffer(buf);

    if (seekable && file_size_opt)
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

using BlobBufPtr = std::unique_ptr<SeekableReadBuffer, void(*)(SeekableReadBuffer*)>;

BlobBufPtr readBlobBytes(
    const PuffinBlob & blob, ReadBuffer & buf, const std::vector<UInt8> & data)
{
    if (auto * seekable = dynamic_cast<SeekableReadBuffer *>(&buf))
    {
        seekable->seek(blob.offset, SEEK_SET);
        return {seekable, [](SeekableReadBuffer*){}};
    }
    return {
        new ReadBufferFromMemory(data.data() + blob.offset, static_cast<size_t>(blob.length)),
        [](SeekableReadBuffer * p){ delete p; }
    };
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

        if (key < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid deletion vector bitmap key: {}", key);
        if (key <= last_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector bitmap keys must be sorted in ascending order");

        while (last_key < key - 1)
            ++last_key;

        roaring::Roaring bitmap;
        try
        {
            bitmap = roaring::Roaring::readSafe(ptr, remaining);
        }
        catch (const std::exception & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to deserialize deletion vector roaring bitmap at key {}: {}", key, e.what());
        }

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

std::vector<UInt64> deserializeDeletionVectorV1(ReadBuffer & buf, size_t size)
{
    String blob_data(size, '\0');
    buf.readStrict(blob_data.data(), size);

    const std::string_view blob_view(blob_data);
    const std::string_view vector_bytes = extractDeletionVectorPayload(blob_view);
    return deserializeRoaringPositionBitmap(vector_bytes);
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
        {"deleted_rows", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
    };
}

}

PuffinMetadataInputFormat::PuffinMetadataInputFormat(ReadBuffer & buf, SharedHeader header_)
    : IInputFormat(std::move(header_), &buf)
{
}

Chunk PuffinMetadataInputFormat::read()
{
    if (!initialized)
    {
        blob_index = 0;
        initialized = true;
        footer = readPuffinFooter(*in);
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

PuffinInputFormat::PuffinInputFormat(ReadBuffer & buf, SharedHeader header_)
    : IInputFormat(std::move(header_), &buf)
{
}

Chunk PuffinInputFormat::read()
{
    if (!initialized)
    {
        blob_index = 0;
        initialized = true;
        footer = readPuffinFooter(*in);
    }
    size_t n = footer.blobs.size();
    if (n == 0 || n <= blob_index)
        return {};

    auto col_rows_data = ColumnUInt64::create();
    auto col_rows_offsets = ColumnArray::ColumnOffsets::create();

    ColumnArray::Offset rows_offset = 0;
    const auto & blob = footer.blobs[blob_index++];

    if (blob.type != "deletion-vector-v1")
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ClickHouse supports only deletion vector blobs. Datasketches deletion vectors are not supported");

    if (!blob.compression_codec.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "deletion-vector-v1 blobs must be uncompressed");

    auto blob_buf = readBlobBytes(blob, *in, footer.data);
    auto rows = deserializeDeletionVectorV1(*blob_buf, static_cast<size_t>(blob.length));

    if (auto cardinality_it = blob.properties.find("cardinality"); cardinality_it != blob.properties.end())
    {
        const UInt64 expected_cardinality = parse<UInt64>(cardinality_it->second);
        if (expected_cardinality != rows.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector cardinality {} does not match deserialized row count {}", expected_cardinality, rows.size());
    }

    size_t elem_count = 0;
    for (UInt64 r : rows)
    {
        ++elem_count;
        col_rows_data->insertValue(r);
    }
    rows_offset += elem_count;
    col_rows_offsets->insertValue(rows_offset);

    auto col_rows = ColumnArray::create(std::move(col_rows_data), std::move(col_rows_offsets));

    MutableColumns cols;
    cols.push_back(std::move(col_rows));
    return Chunk(std::move(cols), 1);
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
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams &, const FormatSettings &)
        { return std::make_shared<PuffinMetadataInputFormat>(buf, std::make_shared<const Block>(sample)); });
    factory.markFormatSupportsSubsetOfColumns("PuffinMetadata");

    factory.registerInputFormat(
        "Puffin",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams &, const FormatSettings &)
        { return std::make_shared<PuffinInputFormat>(buf, std::make_shared<const Block>(sample)); });
    factory.markFormatSupportsSubsetOfColumns("Puffin");
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
