#include <Analyzer/FunctionNode.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Columns/IColumn_fwd.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Core/Range.h>
#include <Core/Settings.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Databases/DataLake/Common.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>
#include <Functions/identity.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Processors/Formats/Impl/AvroRowOutputFormat.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroSchema.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <base/Decimal.h>
#include <base/defines.h>
#include <base/types.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <sys/stat.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/PODArray_fwd.h>
#include <Common/isValidUTF8.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>

#if USE_AVRO

#include <Compiler.hh>
#include <DataFile.hh>
#include <Encoder.hh>
#include <Generic.hh>
#include <GenericDatum.hh>
#include <Specific.hh>
#include <Stream.hh>
#include <ValidSchema.hh>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/String.h>

namespace DB
{

using namespace Iceberg;

namespace Setting
{
    extern const SettingsUInt64 output_format_compression_level;
    extern const SettingsUInt64 output_format_compression_zstd_window_log;
    extern const SettingsBool write_full_path_in_iceberg_metadata;
    extern const SettingsUInt64 iceberg_insert_max_rows_in_data_file;
    extern const SettingsUInt64 iceberg_insert_max_bytes_in_data_file;
}

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
    extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace FailPoints
{
    extern const char iceberg_writes_cleanup[];
}

static constexpr auto MAX_TRANSACTION_RETRIES = 100;

// NOLINTBEGIN(clang-analyzer-core.uninitialized.UndefReturn)
// We work a lot with avro library. Clang analyzer is about GenericDatum structure. It thinks that value in generic datum can be uninitialized.
// No idea why
namespace
{

bool canDumpIcebergStats(const Field & field, DataTypePtr type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::Nullable:
        {
            if (field.isNull())
                return false;
            return canDumpIcebergStats(field, assert_cast<const DataTypeNullable *>(type.get())->getNestedType());
        }
        case TypeIndex::Int32:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::Int64:
        case TypeIndex::DateTime64:
        case TypeIndex::String:
            return true;
        default:
            return false;
    }
}

template <typename T>
std::vector<uint8_t> dumpValue(T value)
{
    std::vector<uint8_t> bytes(sizeof(T));
    std::memcpy(bytes.data(), &value, sizeof(T));
    return bytes;
}

std::vector<uint8_t> dumpFieldToBytes(const Field & field, DataTypePtr type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::Nullable:
            return dumpFieldToBytes(field, assert_cast<const DataTypeNullable *>(type.get())->getNestedType());
        case TypeIndex::Int32:
        case TypeIndex::Date:
        case TypeIndex::Date32:
            return dumpValue(field.safeGet<Int32>());
        case TypeIndex::Int64:
            return dumpValue(field.safeGet<Int64>());
        case TypeIndex::DateTime64:
            return dumpValue(field.safeGet<Decimal64>().getValue().value);
        case TypeIndex::String:
        {
            auto value = field.safeGet<String>();
            std::vector<uint8_t> bytes;
            for (auto elem : value)
                bytes.push_back(elem);
            return bytes;
        }
        case TypeIndex::Float64:
            return dumpValue(field.safeGet<Float64>());
        case TypeIndex::Float32:
            return dumpValue(field.safeGet<Float32>());
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not dump such stats");
        }
    }
}

bool canWriteStatistics(
    const std::vector<std::pair<size_t, Field>> & statistics,
    const std::unordered_map<size_t, size_t> & field_id_to_column_index,
    SharedHeader sample_block)
{
    if (statistics.empty())
        return false;

    for (const auto & [field_id, stat] : statistics)
    {
        auto type = sample_block->getDataTypes()[field_id_to_column_index.at(field_id)];
        if (!canDumpIcebergStats(stat, type))
            return false;
    }
    return true;
}

}

String removeEscapedSlashes(const String & json_str)
{
    auto result = json_str;
    size_t pos = 0;
    while ((pos = result.find("\\/", pos)) != std::string::npos)
    {
        result.replace(pos, 2, "/");
        ++pos;
    }
    return result;
}

void extendSchemaForPartitions(
    String & schema,
    const std::vector<String> & partition_columns,
    const std::vector<DataTypePtr> & partition_types)
{
    Poco::JSON::Array::Ptr partition_fields = new Poco::JSON::Array;
    for (size_t i = 0; i < partition_columns.size(); ++i)
    {
        Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
        field->set(Iceberg::f_field_id, 1000 + i);
        field->set(Iceberg::f_name, partition_columns[i]);
        field->set(Iceberg::f_type, getAvroType(partition_types[i]));
        partition_fields->add(field);
    }

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(partition_fields, oss);

    std::string json_representation = removeEscapedSlashes(oss.str());

    std::string from = "#";
    size_t start_pos = schema.find(from);
    if (start_pos != std::string::npos)
    {
        schema.replace(start_pos, from.size(), json_representation);
    }
}

void generateManifestFile(
    Poco::JSON::Object::Ptr metadata,
    const std::vector<String> & partition_columns,
    const std::vector<Field> & partition_values,
    const std::vector<DataTypePtr> & partition_types,
    const std::vector<String> & data_file_names,
    const std::optional<DataFileStatistics> & data_file_statistics,
    SharedHeader sample_block,
    Poco::JSON::Object::Ptr new_snapshot,
    const String & format,
    Poco::JSON::Object::Ptr partition_spec,
    Int64 partition_spec_id,
    WriteBuffer & buf,
    Iceberg::FileContentType content_type)
{
    Int32 version = metadata->getValue<Int32>(Iceberg::f_format_version);
    String schema_representation;
    if (version == 1)
        schema_representation = manifest_entry_v1_schema;
    else if (version == 2)
        schema_representation = manifest_entry_v2_schema;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown iceberg version {}", version);

    extendSchemaForPartitions(schema_representation, partition_columns, partition_types);
    auto schema = avro::compileJsonSchemaFromString(schema_representation);

    const avro::NodePtr & root_schema = schema.root(); // NOLINT

    if (root_schema->type() != avro::AVRO_RECORD)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg manifest file schema must be record");

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    int current_schema_id = metadata->getValue<Int32>(Iceberg::f_current_schema_id);
    Poco::JSON::Stringifier::stringify(metadata->getArray(Iceberg::f_schemas)->getObject(current_schema_id), oss, 4);

    std::string json_representation = removeEscapedSlashes(oss.str());

    auto adapter = std::make_unique<OutputStreamWriteBufferAdapter>(buf);
    avro::DataFileWriter<avro::GenericDatum> writer(std::move(adapter), schema);
    writer.setMetadata(Iceberg::f_schema, json_representation);

    std::ostringstream oss_partition_spec; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(partition_spec->getArray(Iceberg::f_fields), oss_partition_spec, 4);
    writer.setMetadata(Iceberg::f_partition_spec, oss_partition_spec.str());
    writer.setMetadata(Iceberg::f_partition_spec_id, std::to_string(partition_spec_id));
    for (const auto & data_file_name : data_file_names)
    {
        avro::GenericDatum manifest_datum(root_schema);
        avro::GenericRecord & manifest = manifest_datum.value<avro::GenericRecord>();

        manifest.field(Iceberg::f_status) = avro::GenericDatum(1);
        Int64 snapshot_id = new_snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);

        auto set_versioned_field = [&](const auto & value, const String & field_name)
        {
            if (version > 1)
            {
                size_t field_index;
                if (!schema.root()->nameIndex(field_name, field_index))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found field {} in schema", field_name);

                const avro::NodePtr & union_schema = schema.root()->leafAt(static_cast<UInt32>(field_index));

                avro::GenericUnion field(union_schema);
                field.selectBranch(1);
                field.datum() = avro::GenericDatum(value);
                manifest.field(field_name) = avro::GenericDatum(union_schema, field);
            }
            else
            {
                manifest.field(field_name) = avro::GenericDatum(value);
            }
        };
        set_versioned_field(snapshot_id, Iceberg::f_snapshot_id);

        if (version > 1)
        {
            Int64 sequence_number = new_snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number);

            set_versioned_field(sequence_number, Iceberg::f_sequence_number);
            set_versioned_field(sequence_number, Iceberg::f_file_sequence_number);
        }
        avro::GenericRecord & data_file = manifest.field(Iceberg::f_data_file).value<avro::GenericRecord>();
        if (version > 1)
            data_file.field(Iceberg::f_content) = avro::GenericDatum(static_cast<Int32>(content_type));
        data_file.field(Iceberg::f_file_path) = avro::GenericDatum(data_file_name);
        data_file.field(Iceberg::f_file_format) = avro::GenericDatum(format);

        if (data_file_statistics)
        {
            auto set_fields = [&]<typename T, typename U>(
                                  const std::vector<std::pair<size_t, T>> & statistics, const std::string & field_name, U && dump_function)
            {
                auto & data_file_record = data_file.field(field_name);
                data_file_record.selectBranch(1);
                auto & record_values = data_file_record.value<avro::GenericArray>();
                auto schema_element = record_values.schema()->leafAt(0);
                for (const auto & [field_id, value] : statistics)
                {
                    avro::GenericDatum record_datum(schema_element);
                    auto & record = record_datum.value<avro::GenericRecord>();
                    record.field(Iceberg::f_key) = static_cast<Int64>(field_id);
                    record.field(Iceberg::f_value) = dump_function(field_id, value);
                    record_values.value().push_back(record_datum);
                }
            };

            auto statistics = data_file_statistics->getColumnSizes();
            set_fields(statistics, Iceberg::f_column_sizes, [](size_t, size_t value) { return static_cast<Int64>(value); });

            statistics = data_file_statistics->getNullCounts();
            set_fields(statistics, Iceberg::f_null_value_counts, [](size_t, size_t value) { return static_cast<Int64>(value); });

            std::unordered_map<size_t, size_t> field_id_to_column_index;
            auto field_ids = data_file_statistics->getFieldIds();
            for (size_t i = 0; i < field_ids.size(); ++i)
                field_id_to_column_index[field_ids[i]] = i;

            auto dump_fields = [&](size_t field_id, Field value)
            { return dumpFieldToBytes(value, sample_block->getDataTypes()[field_id_to_column_index.at(field_id)]); };

            auto lower_statistics = data_file_statistics->getLowerBounds();
            if (canWriteStatistics(lower_statistics, field_id_to_column_index, sample_block))
                set_fields(lower_statistics, Iceberg::f_lower_bounds, dump_fields);
            auto upper_statistics = data_file_statistics->getUpperBounds();
            if (canWriteStatistics(upper_statistics, field_id_to_column_index, sample_block))
                set_fields(upper_statistics, Iceberg::f_upper_bounds, dump_fields);
        }
        auto summary = new_snapshot->getObject(Iceberg::f_summary);
        if (summary->has(Iceberg::f_added_records))
        {
            Int64 added_records = summary->getValue<Int64>(Iceberg::f_added_records);
            Int64 added_files_size = summary->getValue<Int64>(Iceberg::f_added_files_size);

            data_file.field(Iceberg::f_record_count) = avro::GenericDatum(added_records);
            data_file.field(Iceberg::f_file_size_in_bytes) = avro::GenericDatum(added_files_size);
        }
        else
        {
            Int64 added_records = summary->getValue<Int64>(Iceberg::f_added_position_deletes);
            Int64 added_files_size = summary->getValue<Int64>(Iceberg::f_added_files_size);

            data_file.field(Iceberg::f_record_count) = avro::GenericDatum(added_records);
            data_file.field(Iceberg::f_file_size_in_bytes) = avro::GenericDatum(added_files_size);
        }
        avro::GenericRecord & partition_record = data_file.field("partition").value<avro::GenericRecord>();
        for (size_t i = 0; i < partition_columns.size(); ++i)
        {
            switch (partition_values[i].getType())
            {
                case Field::Types::Int64:
                case Field::Types::UInt64:
                    partition_record.field(partition_columns[i]) =
                        avro::GenericDatum(partition_values[i].safeGet<Int64>());
                    break;

                case Field::Types::String:
                    partition_record.field(partition_columns[i]) =
                        avro::GenericDatum(partition_values[i].safeGet<String>());
                    break;

                case Field::Types::Float64:
                    partition_record.field(partition_columns[i]) =
                        avro::GenericDatum(partition_values[i].safeGet<Float64>());
                    break;

                case Field::Types::Decimal32:
                    partition_record.field(partition_columns[i]) =
                        avro::GenericDatum(partition_values[i].safeGet<Decimal32>().getValue());
                    break;

                case Field::Types::Decimal64:
                    partition_record.field(partition_columns[i]) =
                        avro::GenericDatum(partition_values[i].safeGet<Decimal64>().getValue());
                    break;

                case Field::Types::Null:
                    break;

                default:
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Unsupported type to write into avro file {}",
                        partition_values[i].getType()
                    );
            }
        }

        writer.write(manifest_datum);
    }
    writer.close();
}

void generateManifestList(
    const FileNamesGenerator & filename_generator,
    Poco::JSON::Object::Ptr metadata,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    const Strings & manifest_entry_names,
    Poco::JSON::Object::Ptr new_snapshot,
    Int64 manifest_length,
    WriteBuffer & buf,
    Iceberg::FileContentType content_type,
    bool use_previous_snapshots)
{
    Int32 version = metadata->getValue<Int32>(Iceberg::f_format_version);
    String schema_representation;
    if (version == 1)
        schema_representation = manifest_list_v1_schema;
    else if (version == 2)
        schema_representation = manifest_list_v2_schema;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown iceberg version {}", version);

    auto schema = avro::compileJsonSchemaFromString(schema_representation); // NOLINT

    auto adapter = std::make_unique<OutputStreamWriteBufferAdapter>(buf);
    avro::DataFileWriter<avro::GenericDatum> writer(std::move(adapter), schema);

    for (const auto & manifest_entry_name : manifest_entry_names)
    {
        avro::GenericDatum entry_datum(schema.root());
        avro::GenericRecord & entry = entry_datum.value<avro::GenericRecord>();

        entry.field(Iceberg::f_manifest_path) = manifest_entry_name;
        entry.field(Iceberg::f_manifest_length) = manifest_length;
        entry.field(Iceberg::f_partition_spec_id) = metadata->getValue<Int64>(Iceberg::f_default_spec_id);
        if (version > 1)
        {
            entry.field(Iceberg::f_content) = static_cast<Int32>(content_type);
            entry.field(Iceberg::f_sequence_number) = new_snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number);
            entry.field(Iceberg::f_min_sequence_number) = new_snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number);
        }

        auto set_versioned_field = [&](const auto & value, const String & field_name)
        {
            if (version == 1)
            {
                size_t field_index;
                if (!schema.root()->nameIndex(field_name, field_index))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found field {} in schema", field_name);

                const avro::NodePtr & union_schema = schema.root()->leafAt(static_cast<UInt32>(field_index));

                avro::GenericUnion field(union_schema);
                field.selectBranch(1);
                field.datum() = avro::GenericDatum(value);
                entry.field(field_name) = avro::GenericDatum(union_schema, field);
            }
            else
            {
                entry.field(field_name) = value;
            }
        };
        entry.field(Iceberg::f_added_snapshot_id) = new_snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        auto summary = new_snapshot->getObject(Iceberg::f_summary);
        if (version == 1)
        {
            set_versioned_field(1, Iceberg::f_added_files_count);
            set_versioned_field(std::stoi(summary->getValue<String>(Iceberg::f_total_data_files)), Iceberg::f_existing_files_count);
            set_versioned_field(0, Iceberg::f_deleted_files_count);
            if (summary->has(Iceberg::f_added_position_deletes))
                set_versioned_field(summary->getValue<Int32>(Iceberg::f_added_position_deletes), Iceberg::f_deleted_rows_count);
        }
        else
        {
            entry.field(Iceberg::f_added_files_count) = 1;
            entry.field(Iceberg::f_existing_files_count)
                = summary->getValue<Int32>(Iceberg::f_total_data_files);
            entry.field(Iceberg::f_deleted_files_count) = 0;

            if (summary->has(Iceberg::f_added_position_deletes))
                entry.field(Iceberg::f_deleted_rows_count) = summary->getValue<Int32>(Iceberg::f_added_position_deletes);
        }

        if (summary->has(Iceberg::f_added_records))
        {
            set_versioned_field(
                summary->getValue<Int32>(Iceberg::f_added_records),
                Iceberg::f_added_rows_count);
        }
        else
        {
            set_versioned_field(summary->getValue<Int32>(Iceberg::f_added_position_deletes), Iceberg::f_added_rows_count);
        }
        set_versioned_field(
            0,
            Iceberg::f_existing_rows_count);
        set_versioned_field(0, Iceberg::f_deleted_rows_count);

        writer.write(entry_datum);
    }

    if (use_previous_snapshots)
    {
        auto parent_snapshot_id = new_snapshot->getValue<Int64>(Iceberg::f_parent_snapshot_id);
        auto snapshots = metadata->getArray(Iceberg::f_snapshots);
        for (size_t i = 0; i < snapshots->size(); ++i)
        {
            if (snapshots->getObject(static_cast<UInt32>(i))->getValue<Int64>(Iceberg::f_metadata_snapshot_id) == parent_snapshot_id)
            {
                auto manifest_list = snapshots->getObject(static_cast<UInt32>(i))->getValue<String>(Iceberg::f_manifest_list);

                RelativePathWithMetadata relative_path_with_metadata(filename_generator.convertMetadataPathToStoragePath(manifest_list));
                auto manifest_list_buf = createReadBuffer(relative_path_with_metadata, object_storage, context, getLogger("IcebergWrites"));

                auto input_stream = std::make_unique<AvroInputStreamReadBufferAdapter>(*manifest_list_buf);
                avro::DataFileReader<avro::GenericDatum> reader(std::move(input_stream));

                const avro::ValidSchema & prev_schema = reader.readerSchema();

                avro::GenericDatum datum(prev_schema);

                while (reader.read(datum))
                {
                    if (version == 1)
                    {
                        const avro::GenericRecord & old_entry = datum.value<avro::GenericRecord>();
                        avro::GenericDatum new_datum(schema.root());
                        avro::GenericRecord & new_entry = new_datum.value<avro::GenericRecord>();
                        new_entry.field(f_manifest_path) = old_entry.field(Iceberg::f_manifest_path);
                        new_entry.field(f_manifest_length) = old_entry.field(Iceberg::f_manifest_length);
                        new_entry.field(f_partition_spec_id) = old_entry.field(Iceberg::f_partition_spec_id);
                        /// Why do we need this for version 1? In some version, iceberg-spark has changed the type of field `f_added_snapshot_id`
                        /// from 'null, long' to 'long'. See https://github.com/apache/iceberg/pull/11626.
                        /// Just in case that we read the old type 'null, long', we do this conversion: read every field
                        /// and write it again with new, correct schema.
                        if (old_entry.hasField(Iceberg::f_added_snapshot_id))
                        {
                            const avro::GenericDatum & old_added_snapshot_id_entry = old_entry.field(Iceberg::f_added_snapshot_id);
                            if (old_added_snapshot_id_entry.isUnion())
                            {
                                if (old_added_snapshot_id_entry.unionBranch() == 0) /// it means add_snapshot_id is null
                                {
                                    /// This only happens when we read data written by a old version of iceberg, which violent the spec of iceberg.
                                    throw Exception(
                                        ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                                        "Manifest list {} has null value for field '{}', but it is required",
                                        relative_path_with_metadata.getPath(),
                                        Iceberg::f_added_snapshot_id);
                                }
                            }
                            new_entry.field(f_added_snapshot_id) = old_added_snapshot_id_entry.value<Int64>();
                        }
                        else
                            /// This only happens when we read data written by a old version of iceberg, which violent the spec of iceberg.
                            throw Exception(
                                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                                "Manifest list {} has null value for field '{}', but it is required",
                                relative_path_with_metadata.getPath(),
                                Iceberg::f_added_snapshot_id);
                        auto add_field_to_datum = [&](const String & field)
                        {
                            if (old_entry.hasField(field))
                                new_entry.field(field) = old_entry.field(field);
                        };
                        add_field_to_datum(Iceberg::f_added_files_count);
                        add_field_to_datum(Iceberg::f_existing_files_count);
                        add_field_to_datum(Iceberg::f_deleted_files_count);
                        add_field_to_datum(Iceberg::f_partitions);
                        add_field_to_datum(Iceberg::f_added_rows_count);
                        add_field_to_datum(Iceberg::f_existing_rows_count);
                        add_field_to_datum(Iceberg::f_deleted_rows_count);
                        add_field_to_datum(Iceberg::f_key_metadata);
                        writer.write(new_datum);
                    }
                    else
                        writer.write(datum);
                }
                break;
            }
        }
    }

    writer.close();
}

IcebergStorageSink::IcebergStorageSink(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_,
    std::shared_ptr<DataLake::ICatalog> catalog_,
    const Iceberg::PersistentTableComponents & persistent_table_components_,
    const StorageID & table_id_)
    : SinkToStorage(sample_block_)
    , sample_block(sample_block_)
    , object_storage(object_storage_)
    , context(context_)
    , format_settings(format_settings_)
    , catalog(catalog_)
    , table_id(table_id_)
    , persistent_table_components(persistent_table_components_)
    , data_lake_settings(configuration_->getDataLakeSettings())
    , write_format(configuration_->format)
    , blob_storage_type_name(configuration_->getTypeName())
    , blob_storage_namespace_name(configuration_->getNamespace())
{
    auto [last_version, metadata_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(
        object_storage,
        persistent_table_components.table_path,
        data_lake_settings,
        persistent_table_components.metadata_cache,
        context_,
        log.get(),
        persistent_table_components.table_uuid);

    metadata = getMetadataJSONObject(
        metadata_path,
        object_storage,
        persistent_table_components.metadata_cache,
        context,
        log,
        compression_method,
        persistent_table_components.table_uuid);
    metadata_compression_method = compression_method;
    auto config_path = persistent_table_components.table_path;
    if (config_path.empty() || config_path.back() != '/')
        config_path += "/";
    if (!config_path.starts_with('/'))
        config_path = '/' + config_path;

    if (!context_->getSettingsRef()[Setting::write_full_path_in_iceberg_metadata])
    {
        filename_generator = FileNamesGenerator(
            config_path, config_path, (catalog != nullptr && catalog->isTransactional()), metadata_compression_method, write_format);
    }
    else
    {
        auto bucket = metadata->getValue<String>(Iceberg::f_location);
        if (bucket.empty() || bucket.back() != '/')
            bucket += "/";
        filename_generator = FileNamesGenerator(
            bucket, config_path, (catalog != nullptr && catalog->isTransactional()), metadata_compression_method, write_format);
    }

    filename_generator.setVersion(last_version + 1);

    partition_spec_id = metadata->getValue<Int64>(Iceberg::f_default_spec_id);
    auto partitions_specs = metadata->getArray(Iceberg::f_partition_specs);

    current_schema_id = metadata->getValue<Int64>(Iceberg::f_current_schema_id);
    auto schemas = metadata->getArray(Iceberg::f_schemas);
    for (size_t i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(static_cast<UInt32>(i))->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(static_cast<UInt32>(i));
        }
    }

    sort_description = Iceberg::getSortingKeyDescriptionFromMetadata(metadata, sample_block->getNamesAndTypesList(), context);

    for (size_t i = 0; i < partitions_specs->size(); ++i)
    {
        auto current_partition_spec = partitions_specs->getObject(static_cast<UInt32>(i));
        if (current_partition_spec->getValue<Int64>(Iceberg::f_spec_id) == partition_spec_id)
        {
            partititon_spec = current_partition_spec;
            Block extended_block_for_sorting = *sample_block_;
            if (!sort_description.column_names.empty())
                sortBlockByKeyDescription(extended_block_for_sorting, sort_description, context);

            if (current_partition_spec->getArray(Iceberg::f_fields)->size() > 0)
                partitioner = ChunkPartitioner(
                    current_partition_spec->getArray(Iceberg::f_fields),
                    current_schema,
                    context_,
                    std::make_shared<const Block>(extended_block_for_sorting));
            break;
        }
    }
}

void IcebergStorageSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;
    total_rows += chunk.getNumRows();

    size_t start_columns_size = chunk.getNumColumns();
    if (!sort_description.column_names.empty())
    {
        ColumnsWithTypeAndName columns;
        for (size_t i = 0; i < chunk.getNumColumns(); ++i)
        {
            columns.push_back(ColumnWithTypeAndName(chunk.getColumns()[i], sample_block->getDataTypes()[i], sample_block->getNames()[i]));
        }
        auto block = Block(columns);
        sortBlockByKeyDescription(block, sort_description, context);

        for (size_t i = 0; i < block.columns(); ++i)
            column_name_to_column_index[block.getNames()[i]] = i;
        auto new_chunk = Chunk(block.getColumns(), block.rows());
        new_chunk.setChunkInfos(chunk.getChunkInfos());
        chunk = std::move(new_chunk);
    }

    std::vector<std::pair<ChunkPartitioner::PartitionKey, Chunk>> partition_result;
    if (partitioner)
        partition_result = partitioner->partitionChunk(chunk);
    else
        partition_result.push_back({{}, chunk.clone()});

    for (const auto & [partition_key, part_chunk] : partition_result)
    {
        if (!writer_per_partition_key.contains(partition_key))
        {
            auto writer = MultipleFileWriter(
                context->getSettingsRef()[Setting::iceberg_insert_max_rows_in_data_file],
                context->getSettingsRef()[Setting::iceberg_insert_max_bytes_in_data_file],
                current_schema->getArray(Iceberg::f_fields),
                filename_generator,
                object_storage,
                context,
                format_settings,
                write_format,
                sample_block);
            writer_per_partition_key.emplace(partition_key, std::move(writer));
        }

        if (!sort_description.column_names.empty() && part_chunk.hasRows() && last_fields_of_last_chunks.contains(partition_key))
        {
            const auto & last_fields = last_fields_of_last_chunks.at(partition_key);
            std::vector<Field> last_fields_new_chunk;
            if (!last_fields.empty())
            {
                bool should_create_new_file = false;
                for (size_t i = 0; i < sort_description.column_names.size(); ++i)
                {
                    auto column_idx = column_name_to_column_index[sort_description.column_names[i]];
                    Field last_field_from_last_chunk = last_fields[i];
                    Field first_field_from_new_chunk;
                    part_chunk.getColumns()[column_idx]->get(0, first_field_from_new_chunk);

                    Field last_field_from_new_chunk;
                    part_chunk.getColumns()[column_idx]->get(part_chunk.getNumRows() - 1, first_field_from_new_chunk);

                    last_fields_new_chunk.push_back(last_field_from_new_chunk);
                    if (sort_description.reverse_flags.empty() || !sort_description.reverse_flags[i])
                    {
                        if (last_field_from_last_chunk > first_field_from_new_chunk)
                        {
                            should_create_new_file = true;
                            break;
                        }
                    }
                    else
                    {
                        if (last_field_from_last_chunk < first_field_from_new_chunk)
                        {
                            should_create_new_file = true;
                            break;
                        }
                    }
                }
                if (should_create_new_file)
                    writer_per_partition_key.at(partition_key).startNewFile();
            }
            last_fields_of_last_chunks[partition_key] = std::move(last_fields_new_chunk);
        }

        auto columns = part_chunk.getColumns();
        columns.resize(start_columns_size);
        Chunk part_chunk_without_sorting_columns(columns, part_chunk.getNumRows());
        writer_per_partition_key.at(partition_key).consume(part_chunk_without_sorting_columns);
    }
    auto columns = chunk.getColumns();
    columns.resize(start_columns_size);
    auto new_chunk = Chunk(columns, chunk.getNumRows());
    new_chunk.setChunkInfos(chunk.getChunkInfos());
    chunk = std::move(new_chunk);
}

void IcebergStorageSink::onFinish()
{
    if (isCancelled())
        return;

    finalizeBuffers();
    releaseBuffers();
}

void IcebergStorageSink::onException(std::exception_ptr /* exception */)
{
    cancelBuffers();
}

void IcebergStorageSink::finalizeBuffers()
{
    for (auto & [partition_key, writer] : writer_per_partition_key)
    {
        writer.finalize();
        total_chunks_size += writer.getResultBytes();
    }

    if (writer_per_partition_key.empty())
        return;

    size_t i = 0;
    while (i < MAX_TRANSACTION_RETRIES)
    {
        if (initializeMetadata())
            break;
        ++i;
    }
}

void IcebergStorageSink::releaseBuffers()
{
    for (auto & [_, writer] : writer_per_partition_key)
    {
        writer.release();
    }
}

void IcebergStorageSink::cancelBuffers()
{
    for (auto & [_, writer] : writer_per_partition_key)
    {
        writer.cancel();
    }
}

bool IcebergStorageSink::initializeMetadata()
{
    auto [metadata_name, storage_metadata_name] = filename_generator.generateMetadataName();

    Int64 parent_snapshot = -1;
    if (metadata->has(Iceberg::f_current_snapshot_id))
        parent_snapshot = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);

    Int32 total_data_files = 0;
    for (const auto & [_, writer] : writer_per_partition_key)
        total_data_files += writer.getDataFiles().size();
    auto [new_snapshot, manifest_list_name, storage_manifest_list_name] = MetadataGenerator(metadata).generateNextMetadata(
        filename_generator, metadata_name, parent_snapshot, total_data_files, total_rows, total_chunks_size, total_data_files, /* added_delete_files */0, /* num_deleted_rows */0);


    Strings manifest_entries_in_storage;
    Strings manifest_entries;
    Int64 manifest_lengths = 0;

    auto cleanup = [&] (bool retry_because_of_metadata_conflict)
    {
        if (!retry_because_of_metadata_conflict)
        {
            for (const auto & [_, writer] : writer_per_partition_key)
                writer.clearAllDataFiles();
        }

        for (const auto & manifest_filename_in_storage : manifest_entries_in_storage)
            object_storage->removeObjectIfExists(StoredObject(manifest_filename_in_storage));

        object_storage->removeObjectIfExists(StoredObject(storage_manifest_list_name));

        if (retry_because_of_metadata_conflict)
        {
            auto [last_version, metadata_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(
                object_storage,
                persistent_table_components.table_path,
                data_lake_settings,
                persistent_table_components.metadata_cache,
                context,
                getLogger("IcebergWrites").get(),
                persistent_table_components.table_uuid);

            LOG_DEBUG(log, "Rereading metadata file {} with version {}", metadata_path, last_version);

            metadata_compression_method = compression_method;
            filename_generator.setVersion(last_version + 1);

            metadata = getMetadataJSONObject(
                metadata_path,
                object_storage,
                persistent_table_components.metadata_cache,
                context,
                getLogger("IcebergWrites"),
                compression_method,
                persistent_table_components.table_uuid);
            partition_spec_id = metadata->getValue<Int64>(Iceberg::f_default_spec_id);
            auto partitions_specs = metadata->getArray(Iceberg::f_partition_specs);

            auto new_schema_id = metadata->getValue<Int64>(Iceberg::f_current_schema_id);
            if (new_schema_id != current_schema_id)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Metadata changed during write operation, try again");

            auto schemas = metadata->getArray(Iceberg::f_schemas);
            for (size_t i = 0; i < schemas->size(); ++i)
            {
                if (schemas->getObject(static_cast<UInt32>(i))->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
                {
                    current_schema = schemas->getObject(static_cast<UInt32>(i));
                }
            }
            for (size_t i = 0; i < partitions_specs->size(); ++i)
            {
                auto current_partition_spec = partitions_specs->getObject(static_cast<UInt32>(i));
                if (current_partition_spec->getValue<Int64>(Iceberg::f_spec_id) == partition_spec_id)
                {
                    partititon_spec = current_partition_spec;
                    if (current_partition_spec->getArray(Iceberg::f_fields)->size() > 0)
                        partitioner = ChunkPartitioner(current_partition_spec->getArray(Iceberg::f_fields), current_schema, context, sample_block);
                    break;
                }
            }
        }
    };

    try
    {
        for (const auto & [partition_key, writer] : writer_per_partition_key)
        {
            auto [manifest_entry_name, storage_manifest_entry_name] = filename_generator.generateManifestEntryName();
            manifest_entries_in_storage.push_back(storage_manifest_entry_name);
            manifest_entries.push_back(manifest_entry_name);

            auto buffer_manifest_entry = object_storage->writeObject(
                StoredObject(storage_manifest_entry_name), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
            try
            {
                generateManifestFile(
                    metadata,
                    partitioner ? partitioner->getColumns() : std::vector<String>{},
                    partition_key,
                    partitioner ? partitioner->getResultTypes() : std::vector<DataTypePtr>{},
                    writer.getDataFiles(),
                    writer.getResultStatistics(),
                    sample_block,
                    new_snapshot,
                    write_format,
                    partititon_spec,
                    partition_spec_id,
                    *buffer_manifest_entry,
                    Iceberg::FileContentType::DATA);
                buffer_manifest_entry->finalize();
                manifest_lengths += buffer_manifest_entry->count();
            }
            catch (...)
            {
                cleanup(false);
                throw;
            }
        }
        {
            auto buffer_manifest_list = object_storage->writeObject(
                StoredObject(storage_manifest_list_name), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

            try
            {
                generateManifestList(
                    filename_generator, metadata, object_storage, context, manifest_entries, new_snapshot, manifest_lengths, *buffer_manifest_list, Iceberg::FileContentType::DATA);
                buffer_manifest_list->finalize();
            }
            catch (...)
            {
                cleanup(false);
                throw;
            }
        }

        {
            std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            Poco::JSON::Stringifier::stringify(metadata, oss, 4);
            std::string json_representation = removeEscapedSlashes(oss.str());

            fiu_do_on(FailPoints::iceberg_writes_cleanup,
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failpoint for cleanup enabled");
            });

            LOG_DEBUG(log, "Writing new metadata file {}", storage_metadata_name);
            auto hint = filename_generator.generateVersionHint();
            if (!writeMetadataFileAndVersionHint(
                    storage_metadata_name,
                    json_representation,
                    hint.path_in_storage,
                    storage_metadata_name,
                    object_storage,
                    context,
                    metadata_compression_method,
                    data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
            {
                LOG_DEBUG(log, "Failed to write metadata {}, retrying", storage_metadata_name);
                cleanup(true);
                return false;
            }
            else
            {
                LOG_DEBUG(log, "Metadata file {} written", storage_metadata_name);
            }

            if (catalog)
            {
                String catalog_filename = metadata_name;
                if (!catalog_filename.starts_with(blob_storage_type_name))
                    catalog_filename = blob_storage_type_name + "://" + blob_storage_namespace_name + "/" + metadata_name;

                const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());
                if (!catalog->updateMetadata(namespace_name, table_name, catalog_filename, new_snapshot))
                {
                    cleanup(true);
                    return false;
                }
            }
        }
    }
    catch (...)
    {
        cleanup(false);
        throw;
    }
    return true;
}

}

// NOLINTEND(clang-analyzer-core.uninitialized.UndefReturn)
#endif
