#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroForIcebergDeserializer.h>

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Common/assert_cast.h>
#include <base/find_symbols.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

AvroForIcebergDeserializer::AvroForIcebergDeserializer(
    std::unique_ptr<ReadBufferFromFileBase> buffer_,
    const std::string & manifest_file_path_,
    const DB::FormatSettings & format_settings)
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

size_t AvroForIcebergDeserializer::rows() const
{
    return parsed_column->size();
}

bool AvroForIcebergDeserializer::hasPath(const std::string & path) const
{
    std::vector<std::string> path_components;
    splitInto<'.'>(path_components, path);

    DataTypePtr current_data_type = parsed_column_data_type;
    for (const auto & subpath : path_components)
    {
        current_data_type = current_data_type->tryGetSubcolumnType(subpath);
        if (current_data_type == nullptr)
            return false;
    }
    return true;
}

TypeIndex AvroForIcebergDeserializer::getTypeForPath(const std::string & path) const
{
    std::vector<std::string> path_components;
    splitInto<'.'>(path_components, path);

    DataTypePtr current_data_type = parsed_column_data_type;
    for (const auto & subpath : path_components)
    {
        current_data_type = current_data_type->tryGetSubcolumnType(subpath);
        if (current_data_type == nullptr)
            throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Key {} doesn't exists in file {}", path, manifest_file_path);
    }
    return WhichDataType(current_data_type).idx;
}

Field AvroForIcebergDeserializer::getValueFromRowByName(size_t row_num, const std::string & path, std::optional<TypeIndex> expected_type) const
{
    std::vector<std::string> path_components;
    splitInto<'.'>(path_components, path);
    ColumnPtr current_column = parsed_column;
    DataTypePtr current_data_type = parsed_column_data_type;
    std::string current_path;
    for (const auto & subpath : path_components)
    {
        if (current_path.empty())
            current_path += subpath;
        else
            current_path += "." + subpath;

        current_column = current_data_type->tryGetSubcolumn(subpath, current_column);
        if (current_column == nullptr)
            throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Key {} doesn't exists in file {}", current_path, manifest_file_path);

        current_data_type = current_data_type->tryGetSubcolumnType(subpath);
    }

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

}

#endif
