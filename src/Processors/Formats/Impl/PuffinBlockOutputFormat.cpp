#include <Processors/Formats/Impl/PuffinBlockOutputFormat.h>
#include <Processors/Formats/Impl/PuffinCommon.h>

#include <limits>
#include <sstream>
#include <zlib.h>
#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <Common/config_version.h>
#include <DataTypes/DataTypeArray.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

Int64 getPosition(const IColumn & positions, size_t index, bool unsigned_positions)
{
    if (unsigned_positions)
    {
        const UInt64 value = positions.getUInt(index);
        if (value > static_cast<UInt64>(DELETION_VECTOR_MAX_POSITION))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Deleted row position {} exceeds the maximum deletion vector position {}",
                value,
                DELETION_VECTOR_MAX_POSITION);
        return static_cast<Int64>(value);
    }

    const Int64 value = positions.getInt(index);
    if (value < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deleted row position {} is negative", value);
    if (value > DELETION_VECTOR_MAX_POSITION)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Deleted row position {} exceeds the maximum deletion vector position {}",
            value,
            DELETION_VECTOR_MAX_POSITION);
    return value;
}

String serializeDeletionVectorBlob(std::map<UInt32, roaring::Roaring> & bitmaps, UInt64 & cardinality)
{
    UInt64 vector_size = sizeof(Int64) + bitmaps.size() * sizeof(Int32);
    cardinality = 0;
    for (auto & [key, bitmap] : bitmaps)
    {
        bitmap.runOptimize();
        vector_size += bitmap.getSizeInBytes(true);
        cardinality += bitmap.cardinality();
    }

    if (sizeof(DELETION_VECTOR_MAGIC) + vector_size > static_cast<UInt64>(std::numeric_limits<Int32>::max()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector of {} bytes does not fit into a Puffin blob", vector_size);

    WriteBufferFromOwnString buf;
    writeBinaryBigEndian(static_cast<UInt32>(sizeof(DELETION_VECTOR_MAGIC) + vector_size), buf);
    buf.write(reinterpret_cast<const char *>(DELETION_VECTOR_MAGIC), sizeof(DELETION_VECTOR_MAGIC));
    writeBinaryLittleEndian(static_cast<Int64>(bitmaps.size()), buf);

    String bitmap_bytes;
    for (const auto & [key, bitmap] : bitmaps)
    {
        writeBinaryLittleEndian(static_cast<Int32>(key), buf);
        bitmap_bytes.resize(bitmap.getSizeInBytes(true));
        bitmap.write(bitmap_bytes.data(), true);
        buf.write(bitmap_bytes.data(), bitmap_bytes.size());
    }

    const std::string_view written = buf.stringView();
    const auto crc = static_cast<UInt32>(crc32_z(
        0L, reinterpret_cast<const unsigned char *>(written.data() + sizeof(UInt32)), written.size() - sizeof(UInt32)));
    writeBinaryBigEndian(crc, buf);
    return buf.str();
}

std::vector<Int32> parseFieldIds(const String & field_ids)
{
    std::vector<Int32> result;
    std::vector<String> parts;
    boost::split(parts, field_ids, boost::is_any_of(","));
    for (auto & part : parts)
    {
        boost::trim(part);
        Int32 field_id = 0;
        if (part.empty() || !tryParse(field_id, part))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Setting output_format_puffin_field_ids must be a comma-separated list of Int32 field ids, got '{}'",
                field_ids);
        result.push_back(field_id);
    }
    return result;
}

}

PuffinBlockOutputFormat::PuffinBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_)
    , referenced_data_file(format_settings_.puffin.referenced_data_file)
    , snapshot_id(format_settings_.puffin.snapshot_id)
    , sequence_number(format_settings_.puffin.sequence_number)
    , field_ids(parseFieldIds(format_settings_.puffin.field_ids))
{
    if (referenced_data_file.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting output_format_puffin_referenced_data_file must be set for the Puffin output format");

    const Block & header = getPort(PortKind::Main).getHeader();
    if (header.columns() != 1)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin output format requires exactly one column with deleted row positions (an integer or an array of integers), got {}",
            header.dumpStructure());

    DataTypePtr positions_type = header.getByPosition(0).type;
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(positions_type.get()))
    {
        positions_are_arrays = true;
        positions_type = array_type->getNestedType();
    }

    WhichDataType which(positions_type);
    if (!which.isNativeUInt() && !which.isNativeInt())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin output format requires a column of deleted row positions of an integer type or an array of integers, got {}",
            header.getByPosition(0).type->getName());
    positions_are_unsigned = which.isNativeUInt();
}

void PuffinBlockOutputFormat::writePrefix()
{
    out.write(reinterpret_cast<const char *>(PUFFIN_MAGIC), sizeof(PUFFIN_MAGIC));
}

void PuffinBlockOutputFormat::addPositions(const IColumn & positions, size_t begin, size_t end)
{
    for (size_t i = begin; i < end; ++i)
    {
        const Int64 position = getPosition(positions, i, positions_are_unsigned);
        bitmaps[static_cast<UInt32>(position >> 32)].add(static_cast<UInt32>(position));
    }
}

void PuffinBlockOutputFormat::consume(Chunk chunk)
{
    const IColumn & column = *chunk.getColumns()[0];
    if (positions_are_arrays)
    {
        const auto & array = assert_cast<const ColumnArray &>(column);
        addPositions(array.getData(), 0, array.getOffsets().back());
    }
    else
    {
        addPositions(column, 0, column.size());
    }
}

void PuffinBlockOutputFormat::finalizeImpl()
{
    UInt64 cardinality = 0;
    const String blob = serializeDeletionVectorBlob(bitmaps, cardinality);
    out.write(blob.data(), blob.size());

    Poco::JSON::Array::Ptr fields_json = new Poco::JSON::Array;
    for (Int32 field_id : field_ids)
        fields_json->add(field_id);

    Poco::JSON::Object::Ptr properties_json = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    properties_json->set("referenced-data-file", referenced_data_file);
    properties_json->set("cardinality", toString(cardinality));

    Poco::JSON::Object::Ptr blob_json = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    blob_json->set("type", PUFFIN_DELETION_VECTOR_BLOB_TYPE);
    blob_json->set("fields", fields_json);
    blob_json->set("snapshot-id", snapshot_id);
    blob_json->set("sequence-number", sequence_number);
    blob_json->set("offset", static_cast<UInt64>(sizeof(PUFFIN_MAGIC)));
    blob_json->set("length", static_cast<UInt64>(blob.size()));
    blob_json->set("properties", properties_json);

    Poco::JSON::Array::Ptr blobs_json = new Poco::JSON::Array;
    blobs_json->add(blob_json);

    Poco::JSON::Object::Ptr file_properties_json = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    file_properties_json->set("created-by", fmt::format("ClickHouse {}", VERSION_STRING));

    Poco::JSON::Object footer_json(Poco::JSON_PRESERVE_KEY_ORDER);
    footer_json.set("blobs", blobs_json);
    footer_json.set("properties", file_properties_json);

    std::ostringstream payload_stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    footer_json.stringify(payload_stream);
    const String payload = payload_stream.str();
    if (payload.size() > static_cast<size_t>(std::numeric_limits<Int32>::max()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin footer of {} bytes is too large", payload.size());

    out.write(reinterpret_cast<const char *>(PUFFIN_MAGIC), sizeof(PUFFIN_MAGIC));
    out.write(payload.data(), payload.size());
    writeBinaryLittleEndian(static_cast<Int32>(payload.size()), out);
    writeBinaryLittleEndian(UInt32{0}, out);
    out.write(reinterpret_cast<const char *>(PUFFIN_MAGIC), sizeof(PUFFIN_MAGIC));
}

void PuffinBlockOutputFormat::resetFormatterImpl()
{
    bitmaps.clear();
}

void registerOutputFormatPuffin(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Puffin",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings, FormatFilterInfoPtr)
        { return std::make_shared<PuffinBlockOutputFormat>(buf, std::make_shared<const Block>(sample), settings); });
    factory.markFormatHasNoAppendSupport("Puffin");
    factory.markOutputFormatNotTTYFriendly("Puffin");
    factory.setContentType("Puffin", "application/octet-stream");
}

}
