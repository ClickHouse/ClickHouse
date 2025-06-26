#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroSchema.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Core/Settings.h>
#include <Storages/ObjectStorage/Utils.h>
#include <base/defines.h>
#include <Interpreters/Context.h>
#include <Formats/FormatFactory.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include "Common/DateLUT.h"
#include "Common/Exception.h"
#include "Common/PODArray_fwd.h"
#include "Common/randomSeed.h"
#include <Common/isValidUTF8.h>
#include "Analyzer/FunctionNode.h"
#include "Columns/ColumnsNumber.h"
#include "Columns/IColumn_fwd.h"
#include "Core/ColumnWithTypeAndName.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "Core/Field.h"
#include "DataTypes/DataTypeString.h"
#include "DataTypes/IDataType.h"
#include "Functions/FunctionFactory.h"
#include "Parsers/ASTFunction.h"
#include "Parsers/Kusto/KustoFunctions/KQLDataTypeFunctions.h"
#include "Storages/ObjectStorage/DataLakes/Iceberg/Constant.h"
#include "base/types.h"
#include <Processors/Formats/Impl/AvroRowOutputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Common/quoteString.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/identity.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>

#include <Compiler.hh>
#include <Generic.hh>
#include <Encoder.hh>
#include <Stream.hh>
#include <DataFile.hh>
#include <Compiler.hh>
#include <GenericDatum.hh>
#include <ValidSchema.hh>
#include <Specific.hh>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/String.h>
#include <memory>
#include <sstream>

#if USE_AVRO

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 output_format_compression_level;
    extern const SettingsUInt64 output_format_compression_zstd_window_log;
}

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
}

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int BAD_ARGUMENTS;
}

FileNamesGenerator::FileNamesGenerator(const String & data_dir_, const String & metadata_dir_)
    : data_dir(data_dir_)
    , metadata_dir(metadata_dir_)
{
}

String FileNamesGenerator::generateDataFileName()
{
    return fmt::format("{}data-{}.parquet", data_dir, uuid_generator.createRandom().toString());
}

String FileNamesGenerator::generateManifestEntryName()
{
    return fmt::format("{}manifest-entry-{}.avro", metadata_dir, uuid_generator.createRandom().toString());
}

String FileNamesGenerator::generateManifestListName()
{
    return fmt::format("{}manifest-list-{}.avro", metadata_dir, uuid_generator.createRandom().toString());
}

String FileNamesGenerator::generateMetadataName()
{
    return fmt::format("{}metadata/{}-{}.metadata.json", metadata_dir, initial_version, uuid_generator.createRandom().toString());
}

std::string removeEscapedSlashes(const std::string& jsonStr) {
    std::string result = jsonStr;
    size_t pos = 0;
    while ((pos = result.find("\\/", pos)) != std::string::npos) {
        result.replace(pos, 2, "/");
        ++pos;
    }
    return result;
}

void extendSchemaForPartitions(
    String & schema,
    const std::vector<String> & partition_columns,
    const std::vector<Field> & partition_values)
{
    Poco::JSON::Array::Ptr partition_fields = new Poco::JSON::Array;
    for (size_t i = 0; i < partition_columns.size(); ++i)
    {
        Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
        field->set("field-id", 1000 + i);
        field->set("name", partition_columns[i]);
        if (partition_values[i].getType() == Field::Types::Int64 || partition_values[i].getType() == Field::Types::UInt64)
            field->set("type", "long");
        else if (partition_values[i].getType() == Field::Types::String)
            field->set("type", "string");
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type for partition {}", partition_values[i].getType());

        partition_fields->add(field);
    }

    std::ostringstream oss;
    Poco::JSON::Stringifier::stringify(partition_fields, oss);

    std::string json_representation = removeEscapedSlashes(oss.str());

    std::string from = "#";
    size_t start_pos = schema.find(from);
    if (start_pos != std::string::npos) {
        schema.replace(start_pos, from.size(), json_representation);
    }
}

void generateManifestFile(
    Poco::JSON::Object::Ptr metadata,
    const std::vector<String> & partition_columns,
    const std::vector<Field> & partition_values,
    const String & data_file_name,
    Poco::JSON::Object::Ptr new_snapshot,
    WriteBuffer & buf)
{
    avro::ValidSchema schema;
    Int32 version = metadata->getValue<Int32>("format-version");
    String schema_representation;
    if (version == 1)
        schema_representation = manifest_entry_v1_schema;
    else if (version == 2)
        schema_representation = manifest_entry_v2_schema;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown iceberg version {}", version);

    extendSchemaForPartitions(schema_representation, partition_columns, partition_values);
    std::cerr << "format version " << version << '\n';
    std::istringstream iss(schema_representation);
    avro::compileJsonSchema(iss, schema);

    const avro::NodePtr &root_schema = schema.root();

    avro::GenericDatum manifest_datum(root_schema);
    avro::GenericRecord& manifest = manifest_datum.value<avro::GenericRecord>();

    manifest.field("status") = avro::GenericDatum(1);
    int64_t snapshot_id = new_snapshot->getValue<int64_t>(MetadataGenerator::f_snapshot_id);

    auto set_versioned_field = [&] (const auto & value, const String & field_name) {
        if (version > 1)
        {
            std::cerr << "set_versioned_field " << field_name << '\n';
            size_t field_index;
            if (!schema.root()->nameIndex(field_name, field_index))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found field {} in schema", field_name);

            const avro::NodePtr& union_schema = schema.root()->leafAt(static_cast<UInt32>(field_index));

            avro::GenericUnion field(union_schema);
            field.selectBranch(1);
            field.datum() = avro::GenericDatum(value);
            manifest.field(field_name) = avro::GenericDatum(union_schema, field);
            std::cerr << "set_versioned_field OK " << field_name << '\n';
        }
        else
        {
            manifest.field(field_name) = avro::GenericDatum(value);
        }
    };
    set_versioned_field(snapshot_id, "snapshot_id");

    if (version > 1)
    {
        int64_t sequence_number = new_snapshot->getValue<int64_t>(MetadataGenerator::f_sequence_number);

        set_versioned_field(sequence_number, Iceberg::f_sequence_number);
        set_versioned_field(sequence_number, "file_sequence_number");
    }
    avro::GenericRecord& data_file = manifest.field("data_file").value<avro::GenericRecord>();

    if (version > 1)
        data_file.field("content") = avro::GenericDatum(static_cast<int32_t>(0));
    data_file.field("file_path") = avro::GenericDatum(data_file_name);
    data_file.field("file_format") = avro::GenericDatum(String("PARQUET"));

    auto summary = new_snapshot->getObject(MetadataGenerator::f_summary);
    int32_t added_records = summary->getValue<int32_t>(MetadataGenerator::f_added_records);
    int32_t added_files_size = summary->getValue<int32_t>(MetadataGenerator::f_added_files_size);

    data_file.field("record_count") = avro::GenericDatum(static_cast<int64_t>(added_records));
    data_file.field("file_size_in_bytes") = avro::GenericDatum(static_cast<int64_t>(added_files_size));

    avro::GenericRecord& partition_record = data_file.field("partition").value<avro::GenericRecord>();
    partition_record.schema()->printJson(std::cerr, 4);
    for (size_t i = 0; i < partition_columns.size(); ++i)
    {
        if (partition_values[i].getType() == Field::Types::Int64 || partition_values[i].getType() == Field::Types::UInt64)
            partition_record.field(partition_columns[i]) = avro::GenericDatum(partition_values[i].safeGet<Int64>());
        else if (partition_values[i].getType() == Field::Types::String)
            partition_record.field(partition_columns[i]) = avro::GenericDatum(partition_values[i].safeGet<String>());
    }
    std::ostringstream oss;
    int current_schema_id = metadata->getValue<Int32>("current-schema-id");
    Poco::JSON::Stringifier::stringify(metadata->getArray("schemas")->getObject(current_schema_id), oss, 4);

    std::string json_representation = removeEscapedSlashes(oss.str());

    auto adapter = std::make_unique<OutputStreamWriteBufferAdapter>(buf);
    avro::DataFileWriter<avro::GenericDatum> writer(std::move(adapter), schema);
    writer.setMetadata("schema", json_representation);
    writer.setMetadata("partition-spec", "[]");
    writer.setMetadata("partition-spec-id", "0");
    writer.writeHeader();
    writer.write(manifest_datum);
    writer.close();
}

void generateManifestList(
    Poco::JSON::Object::Ptr metadata,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    const Strings & manifest_entry_names,
    Poco::JSON::Object::Ptr new_snapshot,
    Int32 manifest_length,
    WriteBuffer & buf)
{
    avro::ValidSchema schema;
    Int32 version = metadata->getValue<Int32>("format-version");
    String schema_representation;
    if (version == 1)
        schema_representation = manifest_list_v1_schema;
    else if (version == 2)
        schema_representation = manifest_list_v2_schema;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown iceberg version {}", version);
    std::istringstream iss(schema_representation);
    avro::compileJsonSchema(iss, schema);

    auto adapter = std::make_unique<OutputStreamWriteBufferAdapter>(buf);
    avro::DataFileWriter<avro::GenericDatum> writer(std::move(adapter), schema);

    writer.writeHeader();

    for (const auto & manifest_entry_name : manifest_entry_names)
    {
        avro::GenericDatum entry_datum(schema.root());
        avro::GenericRecord& entry = entry_datum.value<avro::GenericRecord>();

        entry.field(Iceberg::f_manifest_path) = manifest_entry_name;
        entry.field(Iceberg::f_manifest_length) = manifest_length;
        entry.field(Iceberg::f_partition_spec_id) = 1;
        if (version > 1)
        {
            entry.field(Iceberg::f_content) = 0;
            entry.field(Iceberg::f_sequence_number) = new_snapshot->getValue<Int32>(MetadataGenerator::f_sequence_number);
            entry.field(Iceberg::f_min_sequence_number) = new_snapshot->getValue<Int32>(MetadataGenerator::f_sequence_number);
        }

        auto set_versioned_field = [&] (const auto & value, const String & field_name) {
            if (version == 1)
            {
                size_t field_index;
                if (!schema.root()->nameIndex(field_name, field_index))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found field {} in schema", field_name);

                const avro::NodePtr& union_schema = schema.root()->leafAt(static_cast<UInt32>(field_index));

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
        set_versioned_field(new_snapshot->getValue<Int32>(MetadataGenerator::f_snapshot_id), Iceberg::f_added_snapshot_id);
        if (version > 1)
        {
            entry.field(Iceberg::f_added_files_count) = 1;
            entry.field(Iceberg::f_existing_files_count) = new_snapshot->getObject(MetadataGenerator::f_summary)->getValue<Int32>(MetadataGenerator::f_total_data_files);
            entry.field(Iceberg::f_deleted_files_count) = 0;
        }

        set_versioned_field(new_snapshot->getObject(MetadataGenerator::f_summary)->getValue<Int32>(MetadataGenerator::f_added_records), Iceberg::f_added_rows_count);
        set_versioned_field(new_snapshot->getObject(MetadataGenerator::f_summary)->getValue<Int32>(MetadataGenerator::f_total_records), Iceberg::f_existing_rows_count);
        set_versioned_field(0, Iceberg::f_deleted_rows_count);

        writer.write(entry_datum);
    }
    {
        auto parent_snapshot_id = new_snapshot->getValue<Int64>(MetadataGenerator::f_parent_snapshot_id);
        auto snapshots = metadata->getArray(Iceberg::f_snapshots);
        for (size_t i = 0; i < snapshots->size(); ++i)
        {
            if (snapshots->getObject(static_cast<UInt32>(i))->getValue<Int64>(Iceberg::f_snapshot_id) == parent_snapshot_id)
            {
                auto manifest_list = snapshots->getObject(static_cast<UInt32>(i))->getValue<String>(Iceberg::f_manifest_list);

                StorageObjectStorage::ObjectInfo object_info(manifest_list);
                auto manifest_list_buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, context, nullptr);

                auto input_stream = std::make_unique<AvroInputStreamReadBufferAdapter>(*manifest_list_buf);
                avro::DataFileReader<avro::GenericDatum> reader(std::move(input_stream));

                const avro::ValidSchema& prev_schema = reader.readerSchema();

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
    auto snapshots = metadata_object->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    Int64 max_seq_number = 0;

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto seq_number = snapshot->getValue<Int64>(f_sequence_number);
        max_seq_number = std::max(max_seq_number, seq_number);
    }
    return max_seq_number;
}

Poco::JSON::Object::Ptr MetadataGenerator::getParentSnapshot(Int64 parent_snapshot_id)
{
    auto snapshots = metadata_object->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto snapshot_id = snapshot->getValue<Int64>(f_snapshot_id);
        if (snapshot_id == parent_snapshot_id)
            return snapshot;
    }
    return nullptr;
}

Poco::JSON::Object::Ptr MetadataGenerator::generateNextMetadata(
    const String & manifest_list_name,
    Int64 parent_snapshot_id,
    Int32 added_files,
    Int32 added_records,
    Int32 added_files_size)
{
    int format_version = metadata_object->getValue<Int32>("format-version");
    Poco::JSON::Object::Ptr new_snapshot = new Poco::JSON::Object;
    if (format_version > 1)
        new_snapshot->set(f_sequence_number, getMaxSequenceNumber() + 1);

    Int32 snapshot_id = dis(gen);
    new_snapshot->set(f_snapshot_id, snapshot_id);
    new_snapshot->set(f_parent_snapshot_id, parent_snapshot_id);
    
    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    new_snapshot->set(f_timestamp_ms, ms.count());

    auto parent_snapshot = getParentSnapshot(parent_snapshot_id);
    Poco::JSON::Object::Ptr summary = new Poco::JSON::Object;
    summary->set("operation", "append");
    summary->set(f_added_data_files, added_files);
    summary->set(f_added_records, added_records);
    summary->set(f_added_files_size, added_files_size);
    summary->set(f_changed_partition_count, "1"); // TODO: change this if needs
    
    auto sum_with_parent_snapshot = [&] (const char * field_name, Int32 snapshot_value) {
        Int32 prev_value = parent_snapshot ? parent_snapshot->getObject(f_summary)->getValue<Int32>(field_name) : 0;
        summary->set(field_name, prev_value + snapshot_value);
    };

    sum_with_parent_snapshot(f_total_records, added_records);
    sum_with_parent_snapshot(f_total_files_size, added_files_size);
    sum_with_parent_snapshot(f_total_data_files, added_files);
    sum_with_parent_snapshot(f_total_delete_files, 0);
    sum_with_parent_snapshot(f_total_position_deletes, 0);
    sum_with_parent_snapshot(f_total_equality_deletes, 0);
    new_snapshot->set(f_summary, summary);

    new_snapshot->set(f_schema_id, parent_snapshot ? parent_snapshot->getValue<Int32>(f_schema_id) : 0);
    new_snapshot->set(f_manifest_list, manifest_list_name);

    metadata_object->getArray(f_snapshots)->add(new_snapshot);
    metadata_object->set(Iceberg::f_current_snapshot_id, snapshot_id);
    return new_snapshot;
}

ChunkPartitioner::ChunkPartitioner(
    Poco::JSON::Array::Ptr partition_specification,
    Poco::JSON::Object::Ptr schema,
    ContextPtr context,
    const Block & sample_block_)
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

        auto transform_name = partition_specification_field->getValue<String>(Iceberg::f_transform);
        transform_name = Poco::toLower(transform_name);
        
        FunctionOverloadResolverPtr transform;

        auto source_id = partition_specification_field->getValue<Int32>(Iceberg::f_source_id);
        auto column_name = id_to_column[source_id];

        std::optional<size_t> transform_param;
        auto & factory = FunctionFactory::instance();

        if (transform_name == "year" || transform_name == "years")
            transform = factory.get("toYearNumSinceEpoch", context);

        if (transform_name == "month" || transform_name == "months")
            transform = factory.get("toMonthNumSinceEpoch", context);

        if (transform_name == "day" || transform_name == "date" || transform_name == "days" || transform_name == "dates")
            transform = factory.get("toRelativeDayNum", context);

        if (transform_name == "hour" || transform_name == "hours")
            transform = factory.get("toRelativeHourNum", context);

        if (transform_name == "identity")
            transform = factory.get("identity", context);

        if (transform_name == "void")
            continue;

        if (transform_name.starts_with("truncate") || transform_name.starts_with("bucket"))
        {
            /// should look like transform[N] or bucket[N]
            if (transform_name.back() != ']')
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown transform {}", transform_name);
            }

            auto argument_start = transform_name.find('[');

            if (argument_start == std::string::npos)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown transform {}", transform_name);
            }

            auto argument_width = transform_name.length() - 2 - argument_start;
            std::string argument_string_representation = transform_name.substr(argument_start + 1, argument_width);
            size_t argument;
            bool parsed = DB::tryParse<size_t>(argument, argument_string_representation);

            if (!parsed)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown transform {}", transform_name);
            }

            transform_param = argument;            
            if (transform_name.starts_with("truncate"))
            {
                transform = factory.get("icebergTruncate", context);
            }
            else if (transform_name.starts_with("bucket"))
            {
                transform = factory.get("icebergBucket", context);
            }
        }

        functions.push_back(transform);
        function_params.push_back(transform_param);
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

std::unordered_map<ChunkPartitioner::PartitionKey, Chunk, ChunkPartitioner::PartitionKeyHasher> ChunkPartitioner::participateChunk(const Chunk & chunk)
{
    std::unordered_map<String, ColumnWithTypeAndName> name_to_column;
    for (size_t i = 0; i < sample_block.columns(); ++i)
    {
        auto column_ptr = chunk.getColumns()[i];
        auto column_name = sample_block.getNames()[i];
        name_to_column[column_name] = ColumnWithTypeAndName(
            column_ptr,
            sample_block.getDataTypes()[i],
            column_name
        );
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
        auto result = functions[transform_ind]->build(arguments)->execute(arguments, std::make_shared<DataTypeString>(), chunk.getNumRows(), false);
        for (size_t i = 0; i < chunk.getNumRows(); ++i)
        {
            Field field;
            result->get(i, field);
            transform_results[i].push_back(field.dump());
        }
    }

    std::unordered_map<ChunkPartitioner::PartitionKey, Chunk, ChunkPartitioner::PartitionKeyHasher> result;
    for (const auto & transform_result : transform_results)
        result.insert({transform_result, Chunk{}});

    for (auto & [transform_result, cur_chunk] : result)
    {
        PaddedPODArray<UInt8> mask(chunk.getNumRows());
        for (size_t i = 0; i < chunk.getNumRows(); ++i)
        {
            mask[i] = (transform_results[i] == transform_result);
        }

        for (size_t i = 0; i < chunk.getNumColumns(); ++i)
        {
            cur_chunk.addColumn(chunk.getColumns()[i]->filter(mask, 0));
        }
    }
    return result;
}

IcebergStorageSink::IcebergStorageSink(
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    const std::optional<FormatSettings> & format_settings_,
    const Block & sample_block_,
    ContextPtr context_)
    : SinkToStorage(sample_block_)
    , sample_block(sample_block_)
    , object_storage(object_storage_)
    , context(context_)
    , configuration(configuration_)
    , format_settings(format_settings_)
    , filename_generator(configuration_->getPath(), configuration_->getPath())
{
    configuration->update(object_storage, context, true, false);
    auto log = getLogger("IcebergMetadata");
    auto [last_version, metadata_path] = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration_, nullptr, context_, log.get());

    filename_generator.setVersion(last_version + 1);
    metadata = getMetadataJSONObject(metadata_path, object_storage, configuration, nullptr, context, log);

    Int64 partition_spec_id = metadata->getValue<Int64>("default-spec-id");
    auto partitions_specs = metadata->getArray("partition-specs");

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
        if (current_partition_spec->getValue<Int64>("spec-id") == partition_spec_id)
        {
            partitioner = ChunkPartitioner(current_partition_spec->getArray("fields"), current_schema, context_, sample_block_);
            break;
        }
    }
}

void IcebergStorageSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;
    total_rows += chunk.getNumRows();
    total_chunks_size += chunk.bytes();

    std::unordered_map<ChunkPartitioner::PartitionKey, Chunk, ChunkPartitioner::PartitionKeyHasher> partition_result;
    if (partitioner)
    {
        partition_result = partitioner->participateChunk(chunk);
    }
    else 
    {
        partition_result[{}] = chunk.clone();
    }

    for (const auto & [partition_key, part_chunk] : partition_result)
    {
        if (!data_filenames.contains(partition_key))
        {
            auto data_filename = filename_generator.generateDataFileName();
            data_filenames[partition_key] = data_filename;

            const auto & settings = context->getSettingsRef();
            const auto chosen_compression_method = chooseCompressionMethod(data_filename, configuration->compression_method);

            auto buffer = object_storage->writeObject(
                StoredObject(data_filename), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

            write_buffers[partition_key] = wrapWriteBufferWithCompressionMethod(
                std::move(buffer),
                chosen_compression_method,
                static_cast<int>(settings[Setting::output_format_compression_level]),
                static_cast<int>(settings[Setting::output_format_compression_zstd_window_log]));

            writers[partition_key] = FormatFactory::instance().getOutputFormatParallelIfPossible(
                configuration->format, *write_buffers[partition_key], sample_block, context, format_settings);
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
    }
    initializeMetadata();
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

void IcebergStorageSink::initializeMetadata()
{
    auto manifest_list_name = filename_generator.generateManifestListName();
    auto metadata_name = filename_generator.generateMetadataName();

    Int64 parent_snapshot = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);
    auto new_snapshot = MetadataGenerator(metadata).generateNextMetadata(manifest_list_name, /*parent_snapshot_id TODO*/ parent_snapshot, 1, total_rows, total_chunks_size);

    Strings manifest_entries;
    for (const auto & [partition_key, data_filename] : data_filenames)
    {
        auto manifest_entry_name = filename_generator.generateManifestEntryName();
        manifest_entries.push_back(manifest_entry_name);

        std::cerr << "manifest_entry_name " << manifest_entry_name << ' ' << data_filename << '\n';
        auto buffer_manifest_entry = object_storage->writeObject(
            StoredObject(manifest_entry_name), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
        generateManifestFile(metadata, partitioner->getColumns(), partition_key, data_filename, new_snapshot, *buffer_manifest_entry);
        buffer_manifest_entry->finalize();
    }

    {
        std::cerr << "manifest_list_name " << manifest_list_name << '\n';
        auto buffer_manifest_list = object_storage->writeObject(
            StoredObject(manifest_list_name), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());                    

        generateManifestList(metadata, object_storage, context, manifest_entries, new_snapshot, /*TODO : size of manifest entry*/0, *buffer_manifest_list);
        buffer_manifest_list->finalize();
    }

    {
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(metadata, oss, 4);

        std::string json_representation = removeEscapedSlashes(oss.str());

        auto buffer_metadata = object_storage->writeObject(
            StoredObject(metadata_name), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());                    
        buffer_metadata->write(json_representation.data(), json_representation.size());
        buffer_metadata->finalize();
    }
}

}

#endif
