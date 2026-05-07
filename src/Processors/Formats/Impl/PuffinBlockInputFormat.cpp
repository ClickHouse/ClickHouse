#include <cstring>
#include <unordered_map>
#include <vector>
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
#include "Common/Exception.h"
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
}

namespace
{

constexpr UInt8 PUFFIN_MAGIC[4] = {0x50, 0x46, 0x41, 0x31};

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

roaring::Roaring deserializeRoaring(ReadBuffer & buf, size_t size)
{
    String blob_data(size, '\0');
    buf.readStrict(blob_data.data(), size);

    roaring::Roaring bitmap = roaring::Roaring::readSafe(blob_data.data(), size);
    return bitmap;
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
    if (done)
        return {};
    done = true;

    auto footer = readPuffinFooter(*in);
    size_t n = footer.blobs.size();
    if (n == 0)
        return {};

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

    ColumnArray::Offset fields_offset = 0;
    ColumnArray::Offset props_offset = 0;
    for (const auto & blob : footer.blobs)
    {
        col_type->insertData(blob.type.data(), blob.type.size());
        col_snap->insertValue(blob.snapshot_id);
        col_seq->insertValue(blob.sequence_number);
        for (Int32 f : blob.fields)
            col_fields_data->insertValue(f);
        fields_offset += blob.fields.size();
        col_fields_offsets->insertValue(fields_offset);
        col_offset->insertValue(blob.offset);
        col_length->insertValue(blob.length);
        col_codec->insertData(blob.compression_codec.data(), blob.compression_codec.size());
        for (const auto & [k, v] : blob.properties)
        {
            col_props_keys->insertData(k.data(), k.size());
            col_props_vals->insertData(v.data(), v.size());
        }
        props_offset += blob.properties.size();
        col_props_offsets->insertValue(props_offset);
    }

    auto col_fields = ColumnArray::create(std::move(col_fields_data), std::move(col_fields_offsets));
    MutableColumns prop_cols;
    prop_cols.push_back(std::move(col_props_keys));
    prop_cols.push_back(std::move(col_props_vals));
    MutableColumnPtr col_props_tuple = ColumnTuple::create(std::move(prop_cols));
    MutableColumnPtr col_props_arr = ColumnArray::create(std::move(col_props_tuple), std::move(col_props_offsets));
    MutableColumnPtr col_props = ColumnMap::create(std::move(col_props_arr));

    std::unordered_map<String, MutableColumnPtr> built;
    built.emplace("blob_type", std::move(col_type));
    built.emplace("snapshot_id", std::move(col_snap));
    built.emplace("sequence_number", std::move(col_seq));
    built.emplace("fields", std::move(col_fields));
    built.emplace("offset", std::move(col_offset));
    built.emplace("length", std::move(col_length));
    built.emplace("compression_codec", std::move(col_codec));
    built.emplace("properties", std::move(col_props));

    const Block & out_header = getPort().getHeader();
    MutableColumns result;
    result.reserve(out_header.columns());
    for (const auto & col_with_name : out_header)
        result.push_back(std::move(built.at(col_with_name.name)));
    return Chunk(std::move(result), n);
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

    if (blob.type.find("deletion-vector") == String::npos)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ClickHouse supports only deletion vector blobs. Datasketches deletion vectors are not supported");

    auto blob_buf = readBlobBytes(blob, *in, footer.data);
    auto rows = deserializeRoaring(*blob_buf, static_cast<size_t>(blob.length));
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
    return Chunk(std::move(cols), n);
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
