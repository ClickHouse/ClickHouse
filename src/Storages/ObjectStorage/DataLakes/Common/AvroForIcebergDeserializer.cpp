#include <memory>
#include <sstream>
#include <Core/ColumnWithTypeAndName.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Common/UniqueLock.h>

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Processors/Formats/Impl/JSONObjectEachRowRowOutputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <base/find_symbols.h>
#include <Common/assert_cast.h>

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
    parsed_manifest_file_entries.resize(parsed_column->size());
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
    return extractSubcolumnWithType(path).has_value();
}

TypeIndex AvroForIcebergDeserializer::getTypeForPath(const std::string & path) const
{
    auto & subcolumn_with_type = extractSubcolumnWithType(path);
    if (!subcolumn_with_type.has_value())
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Cannot find column {} in manifest file {}", path, manifest_file_path);
    return WhichDataType(subcolumn_with_type->second).idx;
}

Int64 AvroForIcebergDeserializer::getFormatVersionFromManifestFileMetadata() const
{
    auto format_version_value = tryGetAvroMetadataValue("format-version");
    if (!format_version_value.has_value())
        return 1;
    try
    {
        return std::stoi(format_version_value.value());
    }
    catch (const std::exception & e)
    {
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Cannot read iceberg table format version from Iceberg avro manifest file '{}': {}",
            manifest_file_path,
            e.what());
    }
}


ParsedManifestFileEntryPtr AvroForIcebergDeserializer::createParsedManifestFileEntry(size_t row_index) const
{
    const auto format_version = getFormatVersionFromManifestFileMetadata();
    FileContentType content_type = FileContentType::DATA;
    if (format_version > 1)
        content_type = FileContentType(getValueFromRowByName(row_index, c_data_file_content, TypeIndex::Int32).safeGet<UInt64>());
    const auto status = ManifestEntryStatus(getValueFromRowByName(row_index, f_status, TypeIndex::Int32).safeGet<UInt64>());

    const auto snapshot_id_value = getValueFromRowByName(row_index, f_snapshot_id);
    std::optional<Int64> snapshot_id;

    if (snapshot_id_value.isNull())
    {
        if (status == ManifestEntryStatus::EXISTING)
        {
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Cannot read Iceberg table: manifest file '{}' has entry with status 'EXISTING' without snapshot id",
                manifest_file_path);
        }
    }
    else
    {
        snapshot_id = snapshot_id_value.safeGet<Int64>();
    }

    std::optional<Int64> sequence_number;

    if (format_version > 1)
    {
        const auto sequence_number_value = getValueFromRowByName(row_index, f_sequence_number);
        if (sequence_number_value.isNull())
        {
            if (status == ManifestEntryStatus::EXISTING)
            {
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "Cannot read Iceberg table: manifest file '{}' has entry with status 'EXISTING' without sequence number",
                    manifest_file_path);
            }
        }
        else
        {
            sequence_number = sequence_number_value.safeGet<Int64>();
        }
    }


    const auto file_path_key = getValueFromRowByName(row_index, c_data_file_file_path, TypeIndex::String).safeGet<String>();
    /// NOTE: This is weird, because in manifest file partition looks like this:
    /// {
    /// ...
    ///  "data_file": {
    ///    "partition": {
    ///      "total_amount_trunc": {
    ///        "decimal_10_2": "\u0000\u0000\u0000\u0013<U+0086>"
    ///      }
    ///    },
    ///    ....
    /// However, somehow parser ignores all these nested keys like "total_amount_trunc" or "decimal_10_2" and
    /// directly returns tuple of partition values. However it's exactly what we need.
    Field partition_value = getValueFromRowByName(row_index, c_data_file_partition);
    auto tuple = partition_value.safeGet<Tuple>();

    DB::Row partition_key_value;
    for (const auto & value : tuple)
        partition_key_value.emplace_back(value);

    std::unordered_map<Int32, ColumnInfo> columns_infos;

    for (const auto & path : {c_data_file_value_counts, c_data_file_column_sizes, c_data_file_null_value_counts})
    {
        if (hasPath(path))
        {
            Field values_count = getValueFromRowByName(row_index, path);
            for (const auto & column_stats : values_count.safeGet<Array>())
            {
                const auto & column_number_and_count = column_stats.safeGet<Tuple>();
                Int32 number = static_cast<Int32>(column_number_and_count[0].safeGet<Int32>());
                Int64 count = column_number_and_count[1].safeGet<Int64>();
                if (path == c_data_file_value_counts)
                    columns_infos[number].rows_count = count;
                else if (path == c_data_file_column_sizes)
                    columns_infos[number].bytes_size = count;
                else
                    columns_infos[number].nulls_count = count;
            }
        }
    }

    std::unordered_map<Int32, std::pair<Field, Field>> value_for_bounds;
    for (const auto & path : {c_data_file_lower_bounds, c_data_file_upper_bounds})
    {
        if (hasPath(path))
        {
            Field bounds = getValueFromRowByName(row_index, path);
            for (const auto & column_stats : bounds.safeGet<Array>())
            {
                const auto & column_number_and_bound = column_stats.safeGet<Tuple>();
                Int32 number = static_cast<Int32>(column_number_and_bound[0].safeGet<Int32>());
                const Field & bound_value = column_number_and_bound[1];

                if (!value_for_bounds.contains(number))
                {
                    value_for_bounds[number] = std::make_pair(Field{}, Field{});
                }

                if (path == c_data_file_lower_bounds)
                    value_for_bounds[number].first = bound_value;
                else
                    value_for_bounds[number].second = bound_value;
            }
        }
    }


    String file_format = getValueFromRowByName(row_index, c_data_file_file_format, TypeIndex::String).safeGet<String>();

    std::optional<Int32> sort_order_id;
    if (hasPath(c_data_file_sort_order_id))
    {
        auto sort_order_id_value = getValueFromRowByName(row_index, c_data_file_sort_order_id);
        if (sort_order_id_value.isNull())
            sort_order_id = std::nullopt;
        else
            sort_order_id = sort_order_id_value.safeGet<Int32>();
    }

    switch (content_type)
    {
        case FileContentType::DATA: {
            return std::make_shared<const ParsedManifestFileEntry>(
                FileContentType::DATA,
                file_path_key,
                row_index,
                status,
                sequence_number,
                snapshot_id,
                partition_key_value,
                columns_infos,
                value_for_bounds,
                file_format,
                /*lower_reference_data_file_path_ = */ std::nullopt,
                /*upper_reference_data_file_path_ = */ std::nullopt,
                /*equality_ids*/ std::nullopt,
                sort_order_id);
        }
        case FileContentType::POSITION_DELETE: {
            /// reference_file_path can be absent in schema for some reason, though it is present in specification: https://iceberg.apache.org/spec/#manifests
            std::optional<String> lower_reference_data_file_path = std::nullopt;
            std::optional<String> upper_reference_data_file_path = std::nullopt;
            bool bounds_set_by_referenced_data_file = false;
            if (hasPath(c_data_file_referenced_data_file))
            {
                Field reference_file_path_field = getValueFromRowByName(row_index, c_data_file_referenced_data_file);
                if (!reference_file_path_field.isNull())
                {
                    lower_reference_data_file_path = reference_file_path_field.safeGet<String>();
                    upper_reference_data_file_path = reference_file_path_field.safeGet<String>();
                    bounds_set_by_referenced_data_file = true;
                }
            }
            if (!bounds_set_by_referenced_data_file)
            {
                if (auto it = value_for_bounds.find(IcebergPositionDeleteTransform::data_file_path_column_field_id);
                    it != value_for_bounds.end())
                {
                    auto & [lower, upper] = it->second;
                    if (!lower.isNull())
                        lower_reference_data_file_path = lower.safeGet<String>();
                    if (!upper.isNull())
                        upper_reference_data_file_path = upper.safeGet<String>();
                }
            }
            return std::make_shared<const ParsedManifestFileEntry>(
                FileContentType::POSITION_DELETE,
                file_path_key,
                row_index,
                status,
                sequence_number,
                snapshot_id,
                partition_key_value,
                columns_infos,
                value_for_bounds,
                file_format,
                lower_reference_data_file_path,
                upper_reference_data_file_path,
                /*equality_ids*/ std::nullopt,
                /*sort_order_id = */ std::nullopt);
        }
        case FileContentType::EQUALITY_DELETE: {
            std::vector<Int32> equality_ids;
            if (hasPath(c_data_file_equality_ids))
            {
                Field equality_ids_field = getValueFromRowByName(row_index, c_data_file_equality_ids);
                for (const Field & id : equality_ids_field.safeGet<Array>())
                    equality_ids.push_back(static_cast<Int32>(id.safeGet<Int32>()));
            }
            else
                throw Exception(
                    DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "Couldn't find field {} in equality delete file entry",
                    c_data_file_equality_ids);
            return std::make_shared<const ParsedManifestFileEntry>(
                FileContentType::EQUALITY_DELETE,
                file_path_key,
                row_index,
                status,
                sequence_number,
                snapshot_id,
                partition_key_value,
                columns_infos,
                value_for_bounds,
                file_format,
                /*lower_reference_data_file_path_ = */ std::nullopt,
                /*upper_reference_data_file_path_ = */ std::nullopt,
                equality_ids,
                /*sort_order_id = */ std::nullopt);
        }
    }
}


ParsedManifestFileEntryPtr AvroForIcebergDeserializer::getParsedManifestFileEntry(size_t row_index) const
{
    SharedLockGuard lock(cache_mutex);
    if (row_index >= parsed_manifest_file_entries.size())
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Row number {} is out of bounds", row_index);
    if (!parsed_manifest_file_entries[row_index])
    {
        lock.unlock();
        auto entry = createParsedManifestFileEntry(row_index);
        UniqueLock exclusive_lock(cache_mutex);
        if (!parsed_manifest_file_entries[row_index])
            parsed_manifest_file_entries[row_index] = std::move(entry);
        return parsed_manifest_file_entries[row_index];
    }
    return parsed_manifest_file_entries[row_index];
}

std::optional<std::pair<ColumnPtr, DataTypePtr>> & AvroForIcebergDeserializer::extractSubcolumnWithType(const std::string & path) const
{
    // First, try to read from cache with shared lock (for concurrent reads)
    {
        SharedLockGuard lock(cache_mutex);
        auto it = cache_extracted_subcolumns_with_types.find(path);
        if (it != cache_extracted_subcolumns_with_types.end())
            return it->second;
    }

    // Not in cache, acquire exclusive lock to populate cache
    UniqueLock lock(cache_mutex);

    // Double-check: another thread might have populated the cache while we were waiting for the lock
    auto it = cache_extracted_subcolumns_with_types.find(path);
    if (it != cache_extracted_subcolumns_with_types.end())
        return it->second;

    // Populate cache
    if (parsed_column_data_type->hasSubcolumn(path))
    {
        auto column = parsed_column_data_type->getSubcolumn(path, parsed_column);
        auto data_type = parsed_column_data_type->getSubcolumnType(path);
        cache_extracted_subcolumns_with_types[path] = std::make_pair(column, data_type);
    }
    else
    {
        cache_extracted_subcolumns_with_types[path] = std::nullopt;
    }
    return cache_extracted_subcolumns_with_types[path];
}

Field AvroForIcebergDeserializer::getValueFromRowByName(
    size_t row_num, const std::string & path, std::optional<TypeIndex> expected_type) const
{
    auto & subcolumn_with_type = extractSubcolumnWithType(path);
    if (!subcolumn_with_type.has_value())
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Cannot find column {} in manifest file {}", path, manifest_file_path);

    const auto & [current_column, current_data_type] = *subcolumn_with_type;

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

String AvroForIcebergDeserializer::formatRowAsJSON(const ColumnsWithTypeAndName & parsed_columns, size_t row_number) const
{
    WriteBufferFromOwnString buf;
    FormatSettings settings;
    settings.write_statistics = false;
    JSONEachRowRowOutputFormat output_format = JSONEachRowRowOutputFormat(buf, std::make_shared<const Block>(parsed_columns), settings);
    output_format.writeRow({parsed_column}, row_number);
    output_format.finalize();
    auto result_json = buf.str();
    /// result_json looks like '{"":<some_json>}\n'. We need to extract just <some_json>
    return result_json.substr(4, result_json.size() - 6);
}

String AvroForIcebergDeserializer::getContent(size_t row_number) const
{
    // First check with shared lock if cache is already initialized
    {
        SharedLockGuard shared_lock(cache_mutex);
        if (cache_parsed_columns)
            return formatRowAsJSON(cache_parsed_columns.value(), row_number);
    }

    UniqueLock exclusive_lock(cache_mutex);

    // Double-check: another thread might have initialized while we were waiting
    if (!cache_parsed_columns)
        cache_parsed_columns.emplace(ColumnsWithTypeAndName({ColumnWithTypeAndName(parsed_column, parsed_column_data_type, "")}));

    return formatRowAsJSON(cache_parsed_columns.value(), row_number);
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
