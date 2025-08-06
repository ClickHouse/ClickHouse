#include <Analyzer/FunctionNode.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn_fwd.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Databases/DataLake/Common.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Formats/FormatFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>
#include <Functions/identity.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Processors/Formats/Impl/AvroRowOutputFormat.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroSchema.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/ObjectStorage/Utils.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/PODArray_fwd.h>
#include <Common/isValidUTF8.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <Columns/IColumn.h>

#include <memory>
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

namespace Setting
{
extern const SettingsUInt64 output_format_compression_level;
extern const SettingsUInt64 output_format_compression_zstd_window_log;
extern const SettingsBool write_full_path_in_iceberg_metadata;
}

namespace DataLakeStorageSetting
{
extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

FileNamesGenerator::FileNamesGenerator(const String & table_dir_, const String & storage_dir_, bool use_uuid_in_metadata_)
    : table_dir(table_dir_)
    , storage_dir(storage_dir_)
    , data_dir(table_dir + "data/")
    , metadata_dir(table_dir + "metadata/")
    , storage_data_dir(storage_dir + "data/")
    , storage_metadata_dir(storage_dir + "metadata/")
    , use_uuid_in_metadata(use_uuid_in_metadata_)
{
}

FileNamesGenerator & FileNamesGenerator::operator=(const FileNamesGenerator & other)
{
    if (this == &other)
        return *this;

    data_dir = other.data_dir;
    metadata_dir = other.metadata_dir;
    storage_data_dir = other.storage_data_dir;
    storage_metadata_dir = other.storage_metadata_dir;
    initial_version = other.initial_version;

    table_dir = other.table_dir;
    storage_dir = other.storage_dir;
    use_uuid_in_metadata = other.use_uuid_in_metadata;

    return *this;
}

FileNamesGenerator::Result FileNamesGenerator::generateDataFileName()
{
    auto uuid_str = uuid_generator.createRandom().toString();

    return Result{
        .path_in_metadata = fmt::format("{}data-{}.parquet", data_dir, uuid_str),
        .path_in_storage = fmt::format("{}data-{}.parquet", storage_data_dir, uuid_str)
    };
}

FileNamesGenerator::Result FileNamesGenerator::generateManifestEntryName()
{
    auto uuid_str = uuid_generator.createRandom().toString();

    return Result{
        .path_in_metadata = fmt::format("{}{}.avro", metadata_dir, uuid_str),
        .path_in_storage = fmt::format("{}{}.avro", storage_metadata_dir, uuid_str),
    };
}

FileNamesGenerator::Result FileNamesGenerator::generateManifestListName(Int64 snapshot_id, Int32 format_version)
{
    auto uuid_str = uuid_generator.createRandom().toString();

    return Result{
        .path_in_metadata = fmt::format("{}snap-{}-{}-{}.avro", metadata_dir, snapshot_id, format_version, uuid_str),
        .path_in_storage = fmt::format("{}snap-{}-{}-{}.avro", storage_metadata_dir, snapshot_id, format_version, uuid_str),
    };
}

FileNamesGenerator::Result FileNamesGenerator::generateMetadataName()
{
    if (!use_uuid_in_metadata)
    {
        return Result{
            .path_in_metadata = fmt::format("{}v{}.metadata.json", metadata_dir, initial_version),
            .path_in_storage = fmt::format("{}v{}.metadata.json", storage_metadata_dir, initial_version),
        };
    }
    else
    {
        auto uuid_str = uuid_generator.createRandom().toString();
        return Result{
            .path_in_metadata = fmt::format("{}v{}-{}.metadata.json", metadata_dir, initial_version, uuid_str),
            .path_in_storage = fmt::format("{}v{}-{}.metadata.json", storage_metadata_dir, initial_version, uuid_str),
        };

    }
}

FileNamesGenerator::Result FileNamesGenerator::generateVersionHint()
{
    return Result{
        .path_in_metadata = fmt::format("{}version-hint.text", metadata_dir),
        .path_in_storage = fmt::format("{}version-hint.text", storage_metadata_dir),
    };
}

String FileNamesGenerator::convertMetadataPathToStoragePath(const String & metadata_path) const
{
    return storage_dir + metadata_path.substr(table_dir.size());
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

void extendSchemaForPartitions(String & schema, const std::vector<String> & partition_columns, const std::vector<Field> & partition_values)
{
    Poco::JSON::Array::Ptr partition_fields = new Poco::JSON::Array;
    for (size_t i = 0; i < partition_columns.size(); ++i)
    {
        Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
        field->set(Iceberg::f_field_id, 1000 + i);
        field->set(Iceberg::f_name, partition_columns[i]);
        if (partition_values[i].getType() == Field::Types::Int64 || partition_values[i].getType() == Field::Types::UInt64)
            field->set(Iceberg::f_type, "long");
        else if (partition_values[i].getType() == Field::Types::String)
            field->set(Iceberg::f_type, "string");
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type for partition {}", partition_values[i].getType());

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
    const String & data_file_name,
    Poco::JSON::Object::Ptr new_snapshot,
    const String & format,
    Poco::JSON::Object::Ptr partition_spec,
    Int64 partition_spec_id,
    WriteBuffer & buf)
{
    Int32 version = metadata->getValue<Int32>(Iceberg::f_format_version);
    String schema_representation;
    if (version == 1)
        schema_representation = manifest_entry_v1_schema;
    else if (version == 2)
        schema_representation = manifest_entry_v2_schema;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown iceberg version {}", version);

    extendSchemaForPartitions(schema_representation, partition_columns, partition_values);
    auto schema = avro::compileJsonSchemaFromString(schema_representation);

    const avro::NodePtr & root_schema = schema.root();

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
        data_file.field(Iceberg::f_content) = avro::GenericDatum(0);
    data_file.field(Iceberg::f_file_path) = avro::GenericDatum(data_file_name);
    data_file.field(Iceberg::f_file_format) = avro::GenericDatum(format);

    auto summary = new_snapshot->getObject(Iceberg::f_summary);
    Int64 added_records = summary->getValue<Int64>(Iceberg::f_added_records);
    Int64 added_files_size = summary->getValue<Int64>(Iceberg::f_added_files_size);

    data_file.field(Iceberg::f_record_count) = avro::GenericDatum(added_records);
    data_file.field(Iceberg::f_file_size_in_bytes) = avro::GenericDatum(added_files_size);

    avro::GenericRecord & partition_record = data_file.field("partition").value<avro::GenericRecord>();
    for (size_t i = 0; i < partition_columns.size(); ++i)
    {
        if (partition_values[i].getType() == Field::Types::Int64 || partition_values[i].getType() == Field::Types::UInt64)
            partition_record.field(partition_columns[i]) = avro::GenericDatum(partition_values[i].safeGet<Int64>());
        else if (partition_values[i].getType() == Field::Types::String)
            partition_record.field(partition_columns[i]) = avro::GenericDatum(partition_values[i].safeGet<String>());
    }

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
    writer.write(manifest_datum);
    writer.close();
}

void generateManifestList(
    const FileNamesGenerator & filename_generator,
    Poco::JSON::Object::Ptr metadata,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    const Strings & manifest_entry_names,
    Poco::JSON::Object::Ptr new_snapshot,
    Int32 manifest_length,
    WriteBuffer & buf)
{
    Int32 version = metadata->getValue<Int32>(Iceberg::f_format_version);
    String schema_representation;
    if (version == 1)
        schema_representation = manifest_list_v1_schema;
    else if (version == 2)
        schema_representation = manifest_list_v2_schema;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown iceberg version {}", version);
    auto schema = avro::compileJsonSchemaFromString(schema_representation);

    auto adapter = std::make_unique<OutputStreamWriteBufferAdapter>(buf);
    avro::DataFileWriter<avro::GenericDatum> writer(std::move(adapter), schema);

    for (const auto & manifest_entry_name : manifest_entry_names)
    {
        avro::GenericDatum entry_datum(schema.root());
        avro::GenericRecord & entry = entry_datum.value<avro::GenericRecord>();

        entry.field(Iceberg::f_manifest_path) = manifest_entry_name;
        entry.field(Iceberg::f_manifest_length) = manifest_length;
        entry.field(Iceberg::f_partition_spec_id) = metadata->getValue<Int32>(Iceberg::f_default_spec_id);

        if (version > 1)
        {
            entry.field(Iceberg::f_content) = 0;
            entry.field(Iceberg::f_sequence_number) = new_snapshot->getValue<Int32>(Iceberg::f_metadata_sequence_number);
            entry.field(Iceberg::f_min_sequence_number) = new_snapshot->getValue<Int32>(Iceberg::f_metadata_sequence_number);
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
        set_versioned_field(new_snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id), Iceberg::f_added_snapshot_id);

        if (version == 1)
        {
            set_versioned_field(1, Iceberg::f_added_data_files_count);
            set_versioned_field(std::stoi(new_snapshot->getObject(Iceberg::f_summary)->getValue<String>(Iceberg::f_total_data_files)), Iceberg::f_existing_data_files_count);
            set_versioned_field(0, Iceberg::f_deleted_data_files_count);
        }
        else
        {
            entry.field(Iceberg::f_added_files_count) = 1;
            entry.field(Iceberg::f_existing_files_count)
                = new_snapshot->getObject(Iceberg::f_summary)->getValue<Int32>(Iceberg::f_total_data_files);
            entry.field(Iceberg::f_deleted_files_count) = 0;
        }

        set_versioned_field(
            new_snapshot->getObject(Iceberg::f_summary)->getValue<Int32>(Iceberg::f_added_records),
            Iceberg::f_added_rows_count);
        set_versioned_field(
            new_snapshot->getObject(Iceberg::f_summary)->getValue<Int32>(Iceberg::f_total_records),
            Iceberg::f_existing_rows_count);
        set_versioned_field(0, Iceberg::f_deleted_rows_count);

        writer.write(entry_datum);
    }
    {
        auto parent_snapshot_id = new_snapshot->getValue<Int64>(Iceberg::f_parent_snapshot_id);
        auto snapshots = metadata->getArray(Iceberg::f_snapshots);
        for (size_t i = 0; i < snapshots->size(); ++i)
        {
            if (snapshots->getObject(static_cast<UInt32>(i))->getValue<Int64>(Iceberg::f_metadata_snapshot_id) == parent_snapshot_id)
            {
                auto manifest_list = snapshots->getObject(static_cast<UInt32>(i))->getValue<String>(Iceberg::f_manifest_list);

                StorageObjectStorage::ObjectInfo object_info(filename_generator.convertMetadataPathToStoragePath(manifest_list));
                auto manifest_list_buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, context, getLogger("IcebergWrites"));

                auto input_stream = std::make_unique<AvroInputStreamReadBufferAdapter>(*manifest_list_buf);
                avro::DataFileReader<avro::GenericDatum> reader(std::move(input_stream));

                const avro::ValidSchema & prev_schema = reader.readerSchema();

                avro::GenericDatum datum(prev_schema);

                while (reader.read(datum))
                {
                    writer.write(datum);
                }
                break;
            }
        }
    }

    writer.close();
}

MetadataGenerator::MetadataGenerator(Poco::JSON::Object::Ptr metadata_object_)
    : metadata_object(metadata_object_)
    , gen(randomSeed())
    , dis(0, INT32_MAX)
{
}

Int64 MetadataGenerator::getMaxSequenceNumber()
{
    auto snapshots = metadata_object->get(Iceberg::f_snapshots).extract<Poco::JSON::Array::Ptr>();
    Int64 max_seq_number = 0;

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto seq_number = snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number);
        max_seq_number = std::max(max_seq_number, seq_number);
    }
    return max_seq_number;
}

Poco::JSON::Object::Ptr MetadataGenerator::getParentSnapshot(Int64 parent_snapshot_id)
{
    auto snapshots = metadata_object->get(Iceberg::f_snapshots).extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto snapshot_id = snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        if (snapshot_id == parent_snapshot_id)
            return snapshot;
    }
    return nullptr;
}

MetadataGenerator::NextMetadataResult MetadataGenerator::generateNextMetadata(
    FileNamesGenerator & generator,
    const String & metadata_filename,
    Int64 parent_snapshot_id,
    Int32 added_files,
    Int32 added_records,
    Int32 added_files_size,
    Int32 num_partitions)
{
    int format_version = metadata_object->getValue<Int32>(Iceberg::f_format_version);
    Poco::JSON::Object::Ptr new_snapshot = new Poco::JSON::Object;
    if (format_version > 1)
    {
        auto sequence_number = getMaxSequenceNumber() + 1;
        new_snapshot->set(Iceberg::f_metadata_sequence_number, getMaxSequenceNumber() + 1);
        metadata_object->set(Iceberg::f_last_sequence_number, sequence_number);
    }
    Int32 snapshot_id = dis(gen);

    auto [manifest_list_name, storage_manifest_list_name] = generator.generateManifestListName(snapshot_id, format_version);
    new_snapshot->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
    new_snapshot->set(Iceberg::f_parent_snapshot_id, parent_snapshot_id);

    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    new_snapshot->set(Iceberg::f_timestamp_ms, ms.count());

    auto parent_snapshot = getParentSnapshot(parent_snapshot_id);
    Poco::JSON::Object::Ptr summary = new Poco::JSON::Object;
    summary->set(Iceberg::f_operation, Iceberg::f_append);
    summary->set(Iceberg::f_added_data_files, std::to_string(added_files));
    summary->set(Iceberg::f_added_records, std::to_string(added_records));
    summary->set(Iceberg::f_added_files_size, std::to_string(added_files_size));
    summary->set(Iceberg::f_changed_partition_count, std::to_string(num_partitions));

    auto sum_with_parent_snapshot = [&](const char * field_name, Int32 snapshot_value)
    {
        Int32 prev_value = parent_snapshot ? std::stoi(parent_snapshot->getObject(Iceberg::f_summary)->getValue<String>(field_name)) : 0;
        summary->set(field_name, std::to_string(prev_value + snapshot_value));
    };

    sum_with_parent_snapshot(Iceberg::f_total_records, added_records);
    sum_with_parent_snapshot(Iceberg::f_total_files_size, added_files_size);
    sum_with_parent_snapshot(Iceberg::f_total_data_files, added_files);
    sum_with_parent_snapshot(Iceberg::f_total_delete_files, 0);
    sum_with_parent_snapshot(Iceberg::f_total_position_deletes, 0);
    sum_with_parent_snapshot(Iceberg::f_total_equality_deletes, 0);
    new_snapshot->set(Iceberg::f_summary, summary);

    new_snapshot->set(Iceberg::f_schema_id, parent_snapshot ? parent_snapshot->getValue<Int32>(Iceberg::f_schema_id) : 0);
    new_snapshot->set(Iceberg::f_manifest_list, manifest_list_name);

    metadata_object->getArray(Iceberg::f_snapshots)->add(new_snapshot);
    metadata_object->set(Iceberg::f_current_snapshot_id, snapshot_id);

    if (!metadata_object->has(Iceberg::f_refs))
        metadata_object->set(Iceberg::f_refs, new Poco::JSON::Object);

    if (!metadata_object->getObject(Iceberg::f_refs)->has(Iceberg::f_main))
    {
        Poco::JSON::Object::Ptr branch = new Poco::JSON::Object;
        branch->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
        branch->set(Iceberg::f_type, Iceberg::f_branch);

        metadata_object->getObject(Iceberg::f_refs)->set(Iceberg::f_main, branch);
    }
    else
        metadata_object->getObject(Iceberg::f_refs)->getObject(Iceberg::f_main)->set(Iceberg::f_metadata_snapshot_id, snapshot_id);

    {
        Poco::JSON::Object::Ptr new_metadata_item = new Poco::JSON::Object;
        new_metadata_item->set(Iceberg::f_metadata_file, metadata_filename);
        new_metadata_item->set(Iceberg::f_timestamp_ms, ms.count());
        metadata_object->getArray(Iceberg::f_metadata_log)->add(new_metadata_item);
    }
    {
        Poco::JSON::Object::Ptr new_snapshot_item = new Poco::JSON::Object;
        new_snapshot_item->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
        new_snapshot_item->set(Iceberg::f_timestamp_ms, ms.count());
        metadata_object->getArray(Iceberg::f_snapshot_log)->add(new_snapshot_item);
    }
    return {new_snapshot, manifest_list_name, storage_manifest_list_name};
}

ChunkPartitioner::ChunkPartitioner(
    Poco::JSON::Array::Ptr partition_specification, Poco::JSON::Object::Ptr schema, ContextPtr context, SharedHeader sample_block_)
    : sample_block(sample_block_)
{
    std::unordered_map<Int32, String> id_to_column;
    {
        auto schema_fields = schema->getArray(Iceberg::f_fields);
        for (size_t i = 0; i < schema_fields->size(); ++i)
        {
            auto field = schema_fields->getObject(static_cast<UInt32>(i));
            id_to_column[field->getValue<Int32>(Iceberg::f_id)] = field->getValue<String>(Iceberg::f_name);
        }
    }
    for (size_t i = 0; i != partition_specification->size(); ++i)
    {
        auto partition_specification_field = partition_specification->getObject(static_cast<UInt32>(i));

        auto transform_name = partition_specification_field->getValue<String>("transform");
        transform_name = Poco::toLower(transform_name);

        FunctionOverloadResolverPtr transform;

        auto source_id = partition_specification_field->getValue<Int32>(Iceberg::f_source_id);
        auto column_name = id_to_column[source_id];

        auto & factory = FunctionFactory::instance();

        auto transform_and_argument = Iceberg::parseTransformAndArgument(transform_name);
        if (!transform_and_argument)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown transform {}", transform_name);

        functions.push_back(factory.get(transform_and_argument->transform_name, context));
        function_params.push_back(transform_and_argument->argument);
        columns_to_apply.push_back(column_name);
    }
}

size_t ChunkPartitioner::PartitionKeyHasher::operator()(const PartitionKey & key) const
{
    size_t result = 0;
    for (const auto & part_key : key)
        result ^= hasher(part_key.dump());
    return result;
}

std::vector<std::pair<ChunkPartitioner::PartitionKey, Chunk>>
ChunkPartitioner::partitionChunk(const Chunk & chunk)
{
    std::unordered_map<String, ColumnWithTypeAndName> name_to_column;
    for (size_t i = 0; i < sample_block->columns(); ++i)
    {
        auto column_ptr = chunk.getColumns()[i];
        auto column_name = sample_block->getNames()[i];
        name_to_column[column_name] = ColumnWithTypeAndName(column_ptr, sample_block->getDataTypes()[i], column_name);
    }

    std::vector<ChunkPartitioner::PartitionKey> transform_results(chunk.getNumRows());
    for (size_t transform_ind = 0; transform_ind < functions.size(); ++transform_ind)
    {
        ColumnsWithTypeAndName arguments;
        if (function_params[transform_ind].has_value())
        {
            auto type = std::make_shared<DataTypeUInt64>();
            auto column_value = ColumnUInt64::create();
            column_value->insert(*function_params[transform_ind]);
            auto const_column = ColumnConst::create(std::move(column_value), chunk.getNumRows());
            arguments.push_back(ColumnWithTypeAndName(const_column->clone(), type, "#"));
        }
        arguments.push_back(name_to_column[columns_to_apply[transform_ind]]);
        auto result
            = functions[transform_ind]->build(arguments)->execute(arguments, std::make_shared<DataTypeString>(), chunk.getNumRows(), false);
        for (size_t i = 0; i < chunk.getNumRows(); ++i)
        {
            Field field;
            result->get(i, field);
            transform_results[i].push_back(field);
        }
    }

    auto get_partition = [&](size_t row_num)
    {
        return transform_results[row_num];
    };

    PODArray<size_t> partition_num_to_first_row;
    IColumn::Selector selector;
    ColumnRawPtrs raw_columns;
    for (const auto & column : chunk.getColumns())
        raw_columns.push_back(column.get());

    buildScatterSelector(raw_columns, partition_num_to_first_row, selector, 0, Context::getGlobalContextInstance());

    size_t partitions_count = partition_num_to_first_row.size();
    std::vector<std::pair<ChunkPartitioner::PartitionKey, MutableColumns>> result_columns;
    result_columns.reserve(partitions_count);

    for (size_t i = 0; i < partitions_count; ++i)
        result_columns.push_back({get_partition(partition_num_to_first_row[i]), chunk.cloneEmptyColumns()});

    for (size_t col = 0; col < chunk.getNumColumns(); ++col)
    {
        if (partitions_count > 1)
        {
            MutableColumns scattered = chunk.getColumns()[col]->scatter(partitions_count, selector);
            for (size_t i = 0; i < partitions_count; ++i)
                result_columns[i].second[col] = std::move(scattered[i]);
        }
        else
        {
            result_columns[0].second[col] = chunk.getColumns()[col]->cloneFinalized();
        }
    }

    std::vector<std::pair<ChunkPartitioner::PartitionKey, Chunk>> result;
    result.reserve(result_columns.size());
    for (auto && [key, partition_columns] : result_columns)
    {
        size_t column_size = partition_columns[0]->size();
        result.push_back({key, Chunk(std::move(partition_columns), column_size)});
    }
    return result;
}

IcebergStorageSink::IcebergStorageSink(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_,
    std::shared_ptr<DataLake::ICatalog> catalog_,
    const StorageID & table_id_)
    : SinkToStorage(sample_block_)
    , sample_block(sample_block_)
    , object_storage(object_storage_)
    , context(context_)
    , configuration(configuration_)
    , format_settings(format_settings_)
    , catalog(catalog_)
    , table_id(table_id_)
{
    configuration->update(object_storage, context, true, false);
    auto log = getLogger("IcebergWrites");
    auto [last_version, metadata_path, compression_method]
        = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration_, nullptr, context_, log.get());

    metadata = getMetadataJSONObject(metadata_path, object_storage, configuration, nullptr, context, log, compression_method);

    auto config_path = configuration_->getPathForWrite().path;
    if (config_path.empty() || config_path.back() != '/')
        config_path += "/";
    if (!context_->getSettingsRef()[Setting::write_full_path_in_iceberg_metadata])
    {
        filename_generator = FileNamesGenerator(config_path, config_path, (catalog != nullptr && catalog->isTransactional()));
    }
    else
    {
        auto bucket = metadata->getValue<String>(Iceberg::f_location);
        if (bucket.empty() || bucket.back() != '/')
            bucket += "/";
        filename_generator = FileNamesGenerator(bucket, config_path, (catalog != nullptr && catalog->isTransactional()));
    }

    filename_generator.setVersion(last_version + 1);

    partition_spec_id = metadata->getValue<Int64>(Iceberg::f_default_spec_id);
    auto partitions_specs = metadata->getArray(Iceberg::f_partition_specs);

    auto current_schema_id = metadata->getValue<Int64>(Iceberg::f_current_schema_id);
    Poco::JSON::Object::Ptr current_schema;
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
                partitioner = ChunkPartitioner(current_partition_spec->getArray(Iceberg::f_fields), current_schema, context_, sample_block_);
            break;
        }
    }
}

void IcebergStorageSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;
    total_rows += chunk.getNumRows();

    std::vector<std::pair<ChunkPartitioner::PartitionKey, Chunk>> partition_result;
    if (partitioner)
        partition_result = partitioner->partitionChunk(chunk);
    else
        partition_result.push_back({{}, chunk.clone()});

    for (const auto & [partition_key, part_chunk] : partition_result)
    {
        if (!data_filenames.contains(partition_key))
        {
            auto [data_filename, data_filename_in_storage] = filename_generator.generateDataFileName();
            data_filenames[partition_key] = data_filename;

            auto buffer = object_storage->writeObject(
                StoredObject(data_filename_in_storage), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

            write_buffers[partition_key] = std::move(buffer);
            if (format_settings)
            {
                format_settings->parquet.write_page_index = true;
                format_settings->parquet.bloom_filter_push_down = true;
                format_settings->parquet.filter_push_down = true;
            }
            writers[partition_key] = FormatFactory::instance().getOutputFormatParallelIfPossible(
                configuration->format, *write_buffers[partition_key], *sample_block, context, format_settings);
        }

        writers[partition_key]->write(getHeader().cloneWithColumns(part_chunk.getColumns()));
    }
}

void IcebergStorageSink::onFinish()
{
    if (isCancelled())
        return;

    finalizeBuffers();
    releaseBuffers();
}

void IcebergStorageSink::finalizeBuffers()
{
    for (const auto & [partition_key, _] : data_filenames)
    {
        try
        {
            writers[partition_key]->flush();
            writers[partition_key]->finalize();
        }
        catch (...)
        {
            /// Stop ParallelFormattingOutputFormat correctly.
            cancelBuffers();
            releaseBuffers();
            throw;
        }

        write_buffers[partition_key]->finalize();
        total_chunks_size += write_buffers[partition_key]->count();
    }

    while (!initializeMetadata())
    {
    }
}

void IcebergStorageSink::releaseBuffers()
{
    for (const auto & [partition_key, _] : data_filenames)
    {
        writers[partition_key].reset();
        write_buffers[partition_key].reset();
    }
}

void IcebergStorageSink::cancelBuffers()
{
    for (const auto & [partition_key, _] : data_filenames)
    {
        if (writers[partition_key])
            writers[partition_key]->cancel();
        if (write_buffers[partition_key])
            write_buffers[partition_key]->cancel();
    }
}

bool IcebergStorageSink::initializeMetadata()
{
    auto [metadata_name, storage_metadata_name] = filename_generator.generateMetadataName();
    Int64 parent_snapshot = -1;
    if (metadata->has(Iceberg::f_current_snapshot_id))
        parent_snapshot = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);

    auto [new_snapshot, manifest_list_name, storage_manifest_list_name] = MetadataGenerator(metadata).generateNextMetadata(
        filename_generator, metadata_name, parent_snapshot, write_buffers.size(), total_rows, total_chunks_size, static_cast<Int32>(data_filenames.size()));

    Strings manifest_entries_in_storage;
    Strings manifest_entries;
    Int32 manifest_lengths = 0;
    for (const auto & [partition_key, data_filename] : data_filenames)
    {
        auto [manifest_entry_name, storage_manifest_entry_name] = filename_generator.generateManifestEntryName();
        manifest_entries_in_storage.push_back(storage_manifest_entry_name);
        manifest_entries.push_back(manifest_entry_name);

        auto buffer_manifest_entry = object_storage->writeObject(
            StoredObject(storage_manifest_entry_name), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
        generateManifestFile(metadata, partitioner ? partitioner->getColumns() : std::vector<String>{}, partition_key, data_filename, new_snapshot, configuration->format, partititon_spec, partition_spec_id, *buffer_manifest_entry);
        buffer_manifest_entry->finalize();
        manifest_lengths += buffer_manifest_entry->count();
    }

    {
        auto buffer_manifest_list = object_storage->writeObject(
            StoredObject(storage_manifest_list_name), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

        generateManifestList(
            filename_generator, metadata, object_storage, context, manifest_entries, new_snapshot, manifest_lengths, *buffer_manifest_list);
        buffer_manifest_list->finalize();
    }

    {
        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(metadata, oss, 4);
        std::string json_representation = removeEscapedSlashes(oss.str());

        auto cleanup = [&] ()
        {
            for (const auto & manifest_filename_in_storage : manifest_entries_in_storage)
                object_storage->removeObjectIfExists(StoredObject(manifest_filename_in_storage));

            object_storage->removeObjectIfExists(StoredObject(storage_manifest_list_name));
        };

        if (object_storage->exists(StoredObject(storage_metadata_name)))
        {
            cleanup();
            return false;
        }

        Iceberg::writeMessageToFile(json_representation, storage_metadata_name, object_storage, context);
        if (configuration->getDataLakeSettings()[DataLakeStorageSetting::iceberg_use_version_hint].value)
        {
            auto filename_version_hint = filename_generator.generateVersionHint();
            Iceberg::writeMessageToFile(storage_metadata_name, filename_version_hint.path_in_storage, object_storage, context);
        }
        if (catalog)
        {
            String catalog_filename = metadata_name;
            if (!catalog_filename.starts_with(configuration->getTypeName()))
                catalog_filename = configuration->getTypeName() + "://" + configuration->getNamespace() + "/" + metadata_name;

            const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());
            if (!catalog->updateMetadata(namespace_name, table_name, catalog_filename, new_snapshot))
            {
                cleanup();
                object_storage->removeObjectIfExists(StoredObject(storage_metadata_name));
                return false;
            }
        }
    }
    return true;
}

}

#endif
