#include <memory>
#include <sstream>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Core/ColumnWithTypeAndName.h>
#include <IO/WriteBufferFromString.h>

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Common/assert_cast.h>
#include <base/find_symbols.h>
#include <Processors/Formats/Impl/JSONObjectEachRowRowOutputFormat.h>

namespace DB::ErrorCodes
{
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
    extern const int INCORRECT_DATA;
}

namespace DB::Iceberg
{

using namespace DB;

AvroForIcebergDeserializer::AvroForIcebergDeserializer(
    std::unique_ptr<ReadBufferFromFileBase> buffer_, const std::string & manifest_file_path_, const DB::FormatSettings & format_settings)
try
    : buffer(std::move(buffer_))
    , manifest_file_path(manifest_file_path_)
{
    auto manifest_file_reader
        = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(*buffer));

    avro::NodePtr root_node = manifest_file_reader->dataSchema().root();
    auto data_type = AvroSchemaReader::avroNodeToDataType(root_node);

    MutableColumns columns;
    columns.push_back(data_type->createColumn());
    AvroDeserializer deserializer(data_type, root_node->name(), manifest_file_reader->dataSchema(), true, true, format_settings);
    manifest_file_reader->init();
    RowReadExtension ext;
    while (manifest_file_reader->hasMore())
    {
        manifest_file_reader->decr();
        deserializer.deserializeRow(columns, manifest_file_reader->decoder(), ext);
    }

    metadata = manifest_file_reader->metadata();
    parsed_column = std::move(columns[0]);
    parsed_column_data_type = std::dynamic_pointer_cast<const DataTypeTuple>(data_type);
}
catch (const std::exception & e)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read Iceberg avro manifest file '{}': {}", manifest_file_path_, e.what());
}

size_t AvroForIcebergDeserializer::rows() const
{
    return parsed_column->size();
}

bool AvroForIcebergDeserializer::hasPath(const std::string & path) const
{
    return parsed_column_data_type->hasSubcolumn(path);
}

TypeIndex AvroForIcebergDeserializer::getTypeForPath(const std::string & path) const
{
    return WhichDataType(parsed_column_data_type->getSubcolumnType(path)).idx;
}

Field AvroForIcebergDeserializer::getValueFromRowByName(size_t row_num, const std::string & path, std::optional<TypeIndex> expected_type) const
{
    auto current_column = parsed_column_data_type->getSubcolumn(path, parsed_column);
    auto current_data_type = parsed_column_data_type->getSubcolumnType(path);

    if (expected_type && WhichDataType(current_data_type).idx != *expected_type)
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                        "Got wrong data type for key {} in manifest file {}, expected {}, got {}",
                        path, manifest_file_path, *expected_type, WhichDataType(current_data_type).idx);
    Field result;
    current_column->get(row_num, result);
    return result;
}

std::optional<std::string> AvroForIcebergDeserializer::tryGetAvroMetadataValue(std::string metadata_key) const
{
    auto it = metadata.find(metadata_key);
    if (it == metadata.end())
        return std::nullopt;

    return std::string{it->second.begin(), it->second.end()};
}

namespace
{

String removeAllSlashes(const String & input)
{
    std::string result;
    result.reserve(input.size());

    for (size_t i = 0; i < input.size(); ++i)
    {
        if (i + 1 < input.size() && input[i] == '\\' && input[i + 1] == '"')
        {
            ++i;
            continue;
        }
        result.push_back(input[i]);
    }
    return result;
}

}

String AvroForIcebergDeserializer::getContent(size_t row_number) const
{
    WriteBufferFromOwnString buf;
    FormatSettings settings;
    settings.write_statistics = false;
    ColumnsWithTypeAndName columns({ColumnWithTypeAndName(parsed_column, parsed_column_data_type, "")});
    JSONEachRowRowOutputFormat output_format = JSONEachRowRowOutputFormat(buf, std::make_shared<const Block>(columns), settings);
    output_format.writeRow({parsed_column}, row_number);
    output_format.finalize();
    auto result_json = buf.str();
    /// result_json looks like '{"":<some_json>}\n'. We need to extract just <some_json>
    return result_json.substr(4, result_json.size() - 6);
}

String AvroForIcebergDeserializer::getMetadataContent() const
{
    Poco::JSON::Object::Ptr metadata_object = new Poco::JSON::Object;
    for (const auto & [key, value] : metadata)
    {
        metadata_object->set(key, String(value.begin(), value.end()));
    }
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    metadata_object->stringify(oss);
    return removeAllSlashes(oss.str());
}

}

#endif
