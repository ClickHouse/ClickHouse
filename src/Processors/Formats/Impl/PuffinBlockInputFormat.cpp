#include <cstring>
#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
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
}

namespace
{

constexpr UInt8 PUFFIN_MAGIC[4] = {0x50, 0x46, 0x41, 0x31};
constexpr UInt32 ROARING_SERIAL_COOKIE = 12346;

struct PuffinBlob
{
    String type;
    Int64 snapshot_id = 0;
    Int64 sequence_number = 0;
    std::vector<Int32> fields;
    Int64 offset = 0;
    Int64 length = 0;
    String compression_codec;
};

struct PuffinFooter
{
    std::vector<PuffinBlob> blobs;
    std::vector<UInt8> data;
};

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

PuffinFooter readPuffinFooter(ReadBuffer & buf)
{
    PuffinFooter result;

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(&buf);
    auto file_size_opt = tryGetFileSizeFromReadBuffer(buf);

    if (seekable && file_size_opt)
    {
        const size_t file_size = *file_size_opt;
        if (file_size < 12)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin file too small");

        seekable->seek(0, SEEK_SET);
        char magic_buf[4];
        seekable->readStrict(magic_buf, 4);
        checkMagic(reinterpret_cast<const UInt8 *>(magic_buf), "header");

        seekable->seek(static_cast<off_t>(file_size - 8), SEEK_SET);
        Int32 footer_length_signed = 0;
        readBinaryLittleEndian(footer_length_signed, *seekable);
        char trailing_buf[4];
        seekable->readStrict(trailing_buf, 4);
        checkMagic(reinterpret_cast<const UInt8 *>(trailing_buf), "trailing");

        if (footer_length_signed <= 0
            || static_cast<size_t>(footer_length_signed) + 8 > file_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin footer length: {}", footer_length_signed);

        const size_t footer_length = static_cast<size_t>(footer_length_signed);
        String footer_json(footer_length, '\0');
        seekable->seek(static_cast<off_t>(file_size - 8 - footer_length), SEEK_SET);
        seekable->readStrict(footer_json.data(), footer_length);

        result.blobs = parseFooterJSON(footer_json, file_size);
    }
    else
    {
        // Fallback: materialize the whole file
        {
            std::vector<UInt8> tmp(DEFAULT_BLOCK_SIZE);
            while (!buf.eof())
            {
                size_t n = buf.read(reinterpret_cast<char *>(tmp.data()), tmp.size());
                result.data.insert(result.data.end(), tmp.data(), tmp.data() + n);
            }
        }

        const auto & data = result.data;
        if (data.size() < 12)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin file too small");

        checkMagic(data.data(), "header");
        checkMagic(data.data() + data.size() - 4, "trailing");

        Int32 footer_length_signed = 0;
        std::memcpy(&footer_length_signed, data.data() + data.size() - 8, 4);
        if (footer_length_signed <= 0
            || static_cast<size_t>(footer_length_signed) + 8 > data.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Puffin footer length: {}", footer_length_signed);

        const size_t footer_length = static_cast<size_t>(footer_length_signed);
        const size_t footer_start = data.size() - 8 - footer_length;
        String footer_json(reinterpret_cast<const char *>(data.data() + footer_start), footer_length);

        result.blobs = parseFooterJSON(footer_json, data.size());
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

std::vector<UInt64> deserializeRoaring(ReadBuffer & buf)
{
    UInt32 cookie;
    readBinaryLittleEndian(cookie, buf);

    bool has_run_containers = (cookie & 0xFFFF) == ROARING_SERIAL_COOKIE;
    UInt32 num_containers;
    if (has_run_containers)
        num_containers = (cookie >> 16) + 1;
    else
        readBinaryLittleEndian(num_containers, buf);

    if (num_containers > 65536)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Roaring bitmap: too many containers ({})", num_containers);

    std::vector<std::pair<UInt16, UInt32>> descs(num_containers);
    for (UInt32 i = 0; i < num_containers; ++i)
    {
        UInt16 key;
        UInt16 card_minus1;
        readBinaryLittleEndian(key, buf);
        readBinaryLittleEndian(card_minus1, buf);
        descs[i] = {key, static_cast<UInt32>(card_minus1) + 1};
    }

    std::vector<bool> run_flags(num_containers, false);
    if (has_run_containers)
    {
        UInt32 bitset_bytes = (num_containers + 7) / 8;
        std::vector<UInt8> flag_bytes(bitset_bytes);
        buf.readStrict(reinterpret_cast<char *>(flag_bytes.data()), bitset_bytes);
        for (UInt32 i = 0; i < num_containers; ++i)
            run_flags[i] = (flag_bytes[i / 8] >> (i % 8)) & 1;
    }
    else
    {
        for (UInt32 i = 0; i < num_containers; ++i)
        {
            UInt32 dummy;
            readBinaryLittleEndian(dummy, buf);
        }
    }

    std::vector<UInt64> result;

    for (UInt32 i = 0; i < num_containers; ++i)
    {
        auto [key, card] = descs[i];
        UInt64 base = static_cast<UInt64>(key) << 16;

        if (run_flags[i])
        {
            UInt16 num_runs;
            readBinaryLittleEndian(num_runs, buf);
            for (UInt16 r = 0; r < num_runs; ++r)
            {
                UInt16 start;
                UInt16 len_minus1;
                readBinaryLittleEndian(start, buf);
                readBinaryLittleEndian(len_minus1, buf);
                for (UInt32 v = start; v <= static_cast<UInt32>(start) + len_minus1; ++v)
                    result.push_back(base | v);
            }
        }
        else if (card <= 4096)
        {
            for (UInt32 j = 0; j < card; ++j)
            {
                UInt16 val;
                readBinaryLittleEndian(val, buf);
                result.push_back(base | val);
            }
        }
        else
        {
            for (UInt32 word_idx = 0; word_idx < 1024; ++word_idx)
            {
                UInt64 word;
                readBinaryLittleEndian(word, buf);
                while (word)
                {
                    UInt32 bit = __builtin_ctzll(word);
                    result.push_back(base | (word_idx * 64 + bit));
                    word &= word - 1;
                }
            }
        }
    }

    return result;
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

    ColumnArray::Offset fields_offset = 0;
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
    }

    auto col_fields = ColumnArray::create(std::move(col_fields_data), std::move(col_fields_offsets));

    MutableColumns cols;
    cols.push_back(std::move(col_type));
    cols.push_back(std::move(col_snap));
    cols.push_back(std::move(col_seq));
    cols.push_back(std::move(col_fields));
    cols.push_back(std::move(col_offset));
    cols.push_back(std::move(col_length));
    cols.push_back(std::move(col_codec));
    return Chunk(std::move(cols), n);
}

PuffinInputFormat::PuffinInputFormat(ReadBuffer & buf, SharedHeader header_)
    : IInputFormat(std::move(header_), &buf)
{
}

Chunk PuffinInputFormat::read()
{
    if (done)
        return {};
    done = true;

    auto footer = readPuffinFooter(*in);
    size_t n = footer.blobs.size();
    if (n == 0)
        return {};

    auto col_rows_data = ColumnUInt64::create();
    auto col_rows_offsets = ColumnArray::ColumnOffsets::create();

    ColumnArray::Offset rows_offset = 0;
    for (const auto & blob : footer.blobs)
    {
        if (blob.type.find("deletion-vector") != String::npos && blob.length > 0)
        {
            auto blob_buf = readBlobBytes(blob, *in, footer.data);
            auto rows = deserializeRoaring(*blob_buf);
            for (UInt64 r : rows)
                col_rows_data->insertValue(r);
            rows_offset += rows.size();
        }
        col_rows_offsets->insertValue(rows_offset);
    }

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
