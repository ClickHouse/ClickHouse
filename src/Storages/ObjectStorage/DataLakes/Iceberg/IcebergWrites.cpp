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
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
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
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Columns/IColumn.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <sys/stat.h>
#include <Poco/JSON/Array.h>
#include <Poco/Dynamic/Var.h>
#include <Common/FailPoint.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/CastOverloadResolver.h>
#include <IO/WriteHelpers.h>
#include <base/Decimal.h>
#include <Core/Range.h>
#include <Core/NamesAndTypes.h>
#include <Core/TypeId.h>

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

Poco::JSON::Object::Ptr deepCopy(Poco::JSON::Object::Ptr obj)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    obj->stringify(oss);

    Poco::JSON::Parser parser;
    auto result = parser.parse(oss.str());
    return result.extract<Poco::JSON::Object::Ptr>();
}

bool checkValidSchemaEvolution(Poco::Dynamic::Var old_type, Poco::Dynamic::Var new_type)
{
    if (old_type.isString() && new_type.isString() && old_type.extract<String>() == new_type.extract<String>())
        return true;

    if (new_type.isString() && new_type.extract<String>() == "long" &&
        old_type.isString() && (old_type.extract<String>() == "long" ||  old_type.extract<String>() == "int"))
    {
        return true;
    }

    if (new_type.isString() && new_type.extract<String>() == "double" &&
        old_type.isString() && (old_type.extract<String>() == "float" ||  old_type.extract<String>() == "double"))
    {
        return true;
    }

    {
        auto old_complex_type = old_type.extract<Poco::JSON::Object::Ptr>();
        auto new_complex_type = new_type.extract<Poco::JSON::Object::Ptr>();

        if (old_complex_type && new_complex_type && old_complex_type->has("precision") && new_complex_type->has("precision") &&
            (old_complex_type->getValue<Int32>("precision") <= new_complex_type->getValue<Int32>("precision") &&
             old_complex_type->getValue<Int32>("scale") <= new_complex_type->getValue<Int32>("scale")))
        {
            return true;
        }
    }

    return false;
}

}

FileNamesGenerator::FileNamesGenerator(
    const String & table_dir_,
    const String & storage_dir_,
    bool use_uuid_in_metadata_,
    CompressionMethod compression_method_,
    const String & format_name_)
    : table_dir(table_dir_)
    , storage_dir(storage_dir_)
    , data_dir(table_dir + "data/")
    , metadata_dir(table_dir + "metadata/")
    , storage_data_dir(storage_dir + "data/")
    , storage_metadata_dir(storage_dir + "metadata/")
    , use_uuid_in_metadata(use_uuid_in_metadata_)
    , compression_method(compression_method_)
    , format_name(boost::to_lower_copy(format_name_))
{
}

FileNamesGenerator::FileNamesGenerator(const FileNamesGenerator & other)
{
    data_dir = other.data_dir;
    metadata_dir = other.metadata_dir;
    storage_data_dir = other.storage_data_dir;
    storage_metadata_dir = other.storage_metadata_dir;
    initial_version = other.initial_version;

    table_dir = other.table_dir;
    storage_dir = other.storage_dir;
    use_uuid_in_metadata = other.use_uuid_in_metadata;
    compression_method = other.compression_method;
    format_name = other.format_name;
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
    compression_method = other.compression_method;
    format_name = other.format_name;

    return *this;
}

FileNamesGenerator::Result FileNamesGenerator::generateDataFileName()
{
    auto uuid_str = uuid_generator.createRandom().toString();

    return Result{
        .path_in_metadata = fmt::format("{}data-{}.{}", data_dir, uuid_str, format_name),
        .path_in_storage = fmt::format("{}data-{}.{}", storage_data_dir, uuid_str, format_name)
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
    auto compression_suffix = toContentEncodingName(compression_method);
    if (!compression_suffix.empty())
        compression_suffix = "." + compression_suffix;
    if (!use_uuid_in_metadata)
    {
        auto res = Result{
            .path_in_metadata = fmt::format("{}v{}{}.metadata.json", metadata_dir, initial_version, compression_suffix),
            .path_in_storage = fmt::format("{}v{}{}.metadata.json", storage_metadata_dir, initial_version, compression_suffix),
        };
        initial_version++;
        return res;
    }
    else
    {
        auto uuid_str = uuid_generator.createRandom().toString();
        auto res = Result{
            .path_in_metadata = fmt::format("{}v{}-{}{}.metadata.json", metadata_dir, initial_version, uuid_str, compression_suffix),
            .path_in_storage = fmt::format("{}v{}-{}{}.metadata.json", storage_metadata_dir, initial_version, uuid_str, compression_suffix),
        };
        initial_version++;
        return res;
    }
}

FileNamesGenerator::Result FileNamesGenerator::generateVersionHint()
{
    return Result{
        .path_in_metadata = fmt::format("{}version-hint.text", metadata_dir),
        .path_in_storage = fmt::format("{}version-hint.text", storage_metadata_dir),
    };
}

FileNamesGenerator::Result FileNamesGenerator::generatePositionDeleteFile()
{
    auto uuid_str = uuid_generator.createRandom().toString();

    return Result{
        .path_in_metadata = fmt::format("{}{}-deletes.{}", data_dir, uuid_str, format_name),
        .path_in_storage = fmt::format("{}{}-deletes.{}", storage_data_dir, uuid_str, format_name)
    };
}

String FileNamesGenerator::convertMetadataPathToStoragePath(const String & metadata_path) const
{
    if (!metadata_path.starts_with(table_dir))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Paths in Iceberg must use a consistent format — either /your/path or s3://your/path. Use the write_full_path_in_iceberg_metadata setting to control this behavior {} {}", metadata_path, table_dir);
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
                    record.field(Iceberg::f_key) = static_cast<Int32>(field_id);
                    record.field(Iceberg::f_value) = dump_function(field_id, value);
                    record_values.value().push_back(record_datum);
                }
            };

            auto statistics = data_file_statistics->getColumnSizes();
            set_fields(statistics, Iceberg::f_column_sizes, [](size_t, size_t value) { return static_cast<Int32>(value); });

            statistics = data_file_statistics->getNullCounts();
            set_fields(statistics, Iceberg::f_null_value_counts, [](size_t, size_t value) { return static_cast<Int32>(value); });

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
    Int32 manifest_length,
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
        entry.field(Iceberg::f_partition_spec_id) = metadata->getValue<Int32>(Iceberg::f_default_spec_id);
        if (version > 1)
        {
            entry.field(Iceberg::f_content) = static_cast<Int32>(content_type);
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
        auto summary = new_snapshot->getObject(Iceberg::f_summary);
        if (version == 1)
        {
            set_versioned_field(1, Iceberg::f_added_data_files_count);
            set_versioned_field(std::stoi(summary->getValue<String>(Iceberg::f_total_data_files)), Iceberg::f_existing_data_files_count);
            set_versioned_field(0, Iceberg::f_deleted_data_files_count);
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
    Int32 num_partitions,
    Int32 added_delete_files,
    Int32 num_deleted_rows,
    std::optional<Int64> user_defined_snapshot_id,
    std::optional<Int64> user_defined_timestamp)
{
    int format_version = metadata_object->getValue<Int32>(Iceberg::f_format_version);
    Poco::JSON::Object::Ptr new_snapshot = new Poco::JSON::Object;
    if (format_version > 1)
    {
        auto sequence_number = getMaxSequenceNumber() + 1;
        new_snapshot->set(Iceberg::f_metadata_sequence_number, getMaxSequenceNumber() + 1);
        metadata_object->set(Iceberg::f_last_sequence_number, sequence_number);
    }
    Int64 snapshot_id = user_defined_snapshot_id.value_or(static_cast<Int64>(dis(gen)));

    auto [manifest_list_name, storage_manifest_list_name] = generator.generateManifestListName(snapshot_id, format_version);
    new_snapshot->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
    new_snapshot->set(Iceberg::f_parent_snapshot_id, parent_snapshot_id);

    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    Int64 timestamp = user_defined_timestamp.value_or(ms.count());
    new_snapshot->set(Iceberg::f_timestamp_ms, timestamp);
    metadata_object->set(Iceberg::f_last_updated_ms, timestamp);

    auto parent_snapshot = getParentSnapshot(parent_snapshot_id);
    Poco::JSON::Object::Ptr summary = new Poco::JSON::Object;
    if (num_deleted_rows == 0)
    {
        summary->set(Iceberg::f_operation, Iceberg::f_append);
        summary->set(Iceberg::f_added_data_files, std::to_string(added_files));
        summary->set(Iceberg::f_added_records, std::to_string(added_records));
        summary->set(Iceberg::f_added_files_size, std::to_string(added_files_size));
        summary->set(Iceberg::f_changed_partition_count, std::to_string(num_partitions));
    }
    else
    {
        summary->set(Iceberg::f_operation, Iceberg::f_overwrite);
        summary->set(Iceberg::f_added_delete_files, std::to_string(added_delete_files));
        summary->set(Iceberg::f_added_position_delete_files, std::to_string(added_delete_files));
        summary->set(Iceberg::f_added_files_size, std::to_string(added_files_size));
        summary->set(Iceberg::f_added_position_deletes, std::to_string(num_deleted_rows));
        summary->set(Iceberg::f_changed_partition_count, std::to_string(num_partitions));
    }

    auto sum_with_parent_snapshot = [&](const char * field_name, Int32 snapshot_value)
    {
        Int32 prev_value = parent_snapshot ? std::stoi(parent_snapshot->getObject(Iceberg::f_summary)->getValue<String>(field_name)) : 0;
        summary->set(field_name, std::to_string(prev_value + snapshot_value));
    };

    sum_with_parent_snapshot(Iceberg::f_total_records, added_records);
    sum_with_parent_snapshot(Iceberg::f_total_files_size, added_files_size);
    sum_with_parent_snapshot(Iceberg::f_total_data_files, added_files);
    sum_with_parent_snapshot(Iceberg::f_total_delete_files, added_delete_files);
    sum_with_parent_snapshot(Iceberg::f_total_position_deletes, num_deleted_rows);
    sum_with_parent_snapshot(Iceberg::f_total_equality_deletes, 0);
    new_snapshot->set(Iceberg::f_summary, summary);

    new_snapshot->set(Iceberg::f_schema_id, metadata_object->getValue<Int32>(Iceberg::f_current_schema_id));
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
        new_metadata_item->set(Iceberg::f_timestamp_ms, timestamp);
        metadata_object->getArray(Iceberg::f_metadata_log)->add(new_metadata_item);
    }
    {
        Poco::JSON::Object::Ptr new_snapshot_item = new Poco::JSON::Object;
        new_snapshot_item->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
        new_snapshot_item->set(Iceberg::f_timestamp_ms, timestamp);
        metadata_object->getArray(Iceberg::f_snapshot_log)->add(new_snapshot_item);
    }

    if (added_delete_files > 0)
    {
        if (!metadata_object->has(Iceberg::f_properties))
        {
            Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
            metadata_object->set(Iceberg::f_properties, properties);
        }
        auto properties = metadata_object->getObject(Iceberg::f_properties);
        properties->set("owner", "root");
        properties->set("write.delete.mode", "merge-on-read");
        properties->set("write.merge.mode", "merge-on-read");
        properties->set("write.update.mode", "merge-on-read");
    }
    return {new_snapshot, manifest_list_name, storage_manifest_list_name};
}

void MetadataGenerator::generateDropColumnMetadata(const String & column_name)
{
    auto current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);
    metadata_object->set(Iceberg::f_current_schema_id, current_schema_id + 1);

    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(i);
            break;
        }
    }

    if (!current_schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found schema with id {}", current_schema_id);
    current_schema = deepCopy(current_schema);

    auto fields = current_schema->getArray(Iceberg::f_fields);
    UInt32 index_to_drop = static_cast<UInt32>(fields->size());
    for (UInt32 i = 0; i < fields->size(); ++i)
    {
        if (fields->getObject(i)->getValue<String>(Iceberg::f_name) == column_name)
        {
            index_to_drop = i;
            break;
        }
    }
    if (index_to_drop == fields->size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found column {}", column_name);
    current_schema->getArray(Iceberg::f_fields)->remove(index_to_drop);
    current_schema->set(Iceberg::f_schema_id, current_schema_id + 1);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

void MetadataGenerator::generateAddColumnMetadata(const String & column_name, DataTypePtr type)
{
    if (!type->isNullable())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg spec doesn't allow to add non-nullable columns");
    auto current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);
    metadata_object->set(Iceberg::f_current_schema_id, current_schema_id + 1);

    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(i);
            break;
        }
    }

    if (!current_schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found schema with id {}", current_schema_id);
    current_schema = deepCopy(current_schema);
    auto last_column_id = metadata_object->getValue<Int32>(Iceberg::f_last_column_id);
    metadata_object->set(Iceberg::f_last_column_id, last_column_id + 1);

    auto new_type = getIcebergType(type, last_column_id);
    Poco::JSON::Object::Ptr new_field = new Poco::JSON::Object;
    new_field->set(Iceberg::f_id, last_column_id + 1);
    new_field->set(Iceberg::f_name, column_name);
    new_field->set(Iceberg::f_required, new_type.second);
    new_field->set(Iceberg::f_type, new_type.first);

    current_schema->getArray(Iceberg::f_fields)->add(new_field);
    current_schema->set(Iceberg::f_schema_id, current_schema_id + 1);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

void MetadataGenerator::generateModifyColumnMetadata(const String & column_name, DataTypePtr type)
{
    auto current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);
    metadata_object->set(Iceberg::f_current_schema_id, current_schema_id + 1);

    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(i);
            break;
        }
    }

    if (!current_schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found schema with id {}", current_schema_id);
    current_schema = deepCopy(current_schema);
    auto last_column_id = metadata_object->getValue<Int32>(Iceberg::f_last_column_id);

    auto new_type = getIcebergType(type, last_column_id);
    auto schema_fields = current_schema->getArray(Iceberg::f_fields);

    for (UInt32 i = 0; i < schema_fields->size(); ++i)
    {
        auto current_field = schema_fields->getObject(i);
        if (current_field->getValue<String>(Iceberg::f_name) == column_name)
        {
            if (!checkValidSchemaEvolution(current_field->get(Iceberg::f_type), new_type.first))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg spec doesn't allow schema evolution to type {}", type->getPrettyName());

            auto old_type = deepCopy(current_field);
            current_field->set(Iceberg::f_type, new_type.first);
            if (!current_field->getValue<bool>(Iceberg::f_required) && !type->isNullable())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg spec doesn't allow change type from nullable to non-nullable {}", type->getPrettyName());

            current_field->set(Iceberg::f_required, new_type.second);
            break;
        }
    }
    current_schema->set(Iceberg::f_schema_id, current_schema_id + 1);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
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

        auto function = factory.get(transform_and_argument->transform_name, context);

        ColumnsWithTypeAndName columns_for_function;
        if (transform_and_argument->argument)
            columns_for_function.push_back(ColumnWithTypeAndName(nullptr, std::make_shared<DataTypeUInt64>(), ""));
        columns_for_function.push_back(sample_block_->getByName(column_name));

        result_data_types.push_back(function->getReturnType(columns_for_function));
        functions.push_back(function);
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
    chassert(partitions_count > 0);
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

DataFileStatistics::DataFileStatistics(Poco::JSON::Array::Ptr schema_)
{
    field_ids.resize(schema_->size());
    for (UInt32 i = 0; i < schema_->size(); ++i)
    {
        auto field = schema_->getObject(i);
        size_t field_id = field->getValue<size_t>(Iceberg::f_id);
        field_ids[i] =  field_id;
    }
}

Range getExtremeRangeFromColumn(const ColumnPtr & column)
{
    Field min_val;
    Field max_val;
    column->getExtremes(min_val, max_val);
    return Range(min_val, true, max_val, true);
}

void DataFileStatistics::update(const Chunk & chunk)
{
    size_t num_columns = chunk.getNumColumns();
    if (column_sizes.empty())
    {
        column_sizes.resize(num_columns, 0);
        null_counts.resize(num_columns, 0);
        for (size_t i = 0; i < num_columns; ++i)
        {
            ranges.push_back(getExtremeRangeFromColumn(chunk.getColumns()[i]));
        }
    }

    chassert(ranges.size() == num_columns);

    for (size_t i = 0; i < num_columns; ++i)
    {
        column_sizes[i] += chunk.getColumns()[i]->byteSize();
        for (size_t j = 0; j < chunk.getNumRows(); ++j)
            null_counts[i] += (chunk.getColumns()[i]->isNullAt(j));
        ranges[i] = uniteRanges(ranges[i], getExtremeRangeFromColumn(chunk.getColumns()[i]));
    }
}

Range DataFileStatistics::uniteRanges(const Range & left, const Range & right)
{
    return Range(
        Range::less(left.left, right.left) ? left.left : right.left,
        true,
        Range::less(right.right, left.right) ? left.right : right.right,
        true);
}

std::vector<std::pair<size_t, size_t>> DataFileStatistics::getColumnSizes() const
{
    std::vector<std::pair<size_t, size_t>> result;
    for (size_t i = 0; i < column_sizes.size(); ++i)
    {
        result.push_back({field_ids[i], column_sizes[i]});
    }
    return result;
}

std::vector<std::pair<size_t, size_t>> DataFileStatistics::getNullCounts() const
{
    std::vector<std::pair<size_t, size_t>> result;
    for (size_t i = 0; i < null_counts.size(); ++i)
    {
        result.push_back({field_ids[i], null_counts[i]});
    }
    return result;
}


std::vector<std::pair<size_t, Field>> DataFileStatistics::getLowerBounds() const
{
    std::vector<std::pair<size_t, Field>> result;
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        result.push_back({field_ids[i], ranges[i].left});
    }
    return result;
}

std::vector<std::pair<size_t, Field>> DataFileStatistics::getUpperBounds() const
{
    std::vector<std::pair<size_t, Field>> result;
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        result.push_back({field_ids[i], ranges[i].right});
    }
    return result;
}

MultipleFileWriter::MultipleFileWriter(
    UInt64 max_data_file_num_rows_,
    UInt64 max_data_file_num_bytes_,
    Poco::JSON::Array::Ptr schema,
    FileNamesGenerator & filename_generator_,
    ObjectStoragePtr object_storage_,
    ContextPtr context_,
    const std::optional<FormatSettings> & format_settings_,
    StorageObjectStorageConfigurationPtr configuration_,
    SharedHeader sample_block_)
    : max_data_file_num_rows(max_data_file_num_rows_)
    , max_data_file_num_bytes(max_data_file_num_bytes_)
    , stats(schema)
    , filename_generator(filename_generator_)
    , object_storage(object_storage_)
    , context(context_)
    , format_settings(format_settings_)
    , configuration(configuration_)
    , sample_block(sample_block_)
{
}

void MultipleFileWriter::consume(const Chunk & chunk)
{
    if (!current_file_num_rows || *current_file_num_rows >= max_data_file_num_rows || *current_file_num_bytes >= max_data_file_num_bytes)
    {
        if (buffer)
            finalize();

        current_file_num_rows = 0;
        current_file_num_bytes = 0;
        auto filename = filename_generator.generateDataFileName();

        data_file_names.push_back(filename.path_in_storage);
        buffer = object_storage->writeObject(
            StoredObject(filename.path_in_storage), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

        if (format_settings)
        {
            format_settings->parquet.write_page_index = true;
            format_settings->parquet.bloom_filter_push_down = true;
            format_settings->parquet.filter_push_down = true;
        }
        output_format = FormatFactory::instance().getOutputFormatParallelIfPossible(
            configuration->format, *buffer, *sample_block, context, format_settings);
    }
    output_format->write(sample_block->cloneWithColumns(chunk.getColumns()));
    output_format->flush();
    *current_file_num_rows += chunk.getNumRows();
    *current_file_num_bytes += chunk.bytes();
    stats.update(chunk);
}

void MultipleFileWriter::finalize()
{
    output_format->flush();
    output_format->finalize();
    buffer->finalize();
    total_bytes += buffer->count();
}

void MultipleFileWriter::release()
{
    output_format.reset();
    buffer.reset();
}

void MultipleFileWriter::cancel()
{
    output_format->cancel();
    buffer->cancel();
}

void MultipleFileWriter::clearAllDataFiles() const
{
    for (const auto & data_filename : data_file_names)
        object_storage->removeObjectIfExists(StoredObject(data_filename));
}

UInt64 MultipleFileWriter::getResultBytes() const
{
    return total_bytes;
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
    configuration->update(object_storage, context, /* if_not_updated_before */ true);
    auto [last_version, metadata_path, compression_method]
        = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration_, nullptr, context_, log.get());

    metadata = getMetadataJSONObject(metadata_path, object_storage, configuration, nullptr, context, log, compression_method);
    metadata_compression_method = compression_method;
    auto config_path = configuration_->getPathForWrite().path;
    if (config_path.empty() || config_path.back() != '/')
        config_path += "/";
    if (!config_path.starts_with('/'))
        config_path = '/' + config_path;

    if (!context_->getSettingsRef()[Setting::write_full_path_in_iceberg_metadata])
    {
        filename_generator = FileNamesGenerator(config_path, config_path, (catalog != nullptr && catalog->isTransactional()), metadata_compression_method, configuration_->format);
    }
    else
    {
        auto bucket = metadata->getValue<String>(Iceberg::f_location);
        if (bucket.empty() || bucket.back() != '/')
            bucket += "/";
        filename_generator = FileNamesGenerator(bucket, config_path, (catalog != nullptr && catalog->isTransactional()), metadata_compression_method, configuration_->format);
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
                configuration,
                sample_block);
            writer_per_partition_key.emplace(partition_key, std::move(writer));
        }

        writer_per_partition_key.at(partition_key).consume(part_chunk);
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
    Int32 manifest_lengths = 0;

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
            auto [last_version, metadata_path, compression_method]
                = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration, nullptr, context, getLogger("IcebergWrites").get());

            LOG_DEBUG(log, "Rereading metadata file {} with version {}", metadata_path, last_version);

            metadata_compression_method = compression_method;
            filename_generator.setVersion(last_version + 1);

            metadata = getMetadataJSONObject(metadata_path, object_storage, configuration, nullptr, context, getLogger("IcebergWrites"), compression_method);
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
                    configuration->format,
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
            if (!writeMetadataFileAndVersionHint(storage_metadata_name, json_representation, hint.path_in_storage, storage_metadata_name, object_storage, context, metadata_compression_method, configuration->getDataLakeSettings()[DataLakeStorageSetting::iceberg_use_version_hint]))
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
                if (!catalog_filename.starts_with(configuration->getTypeName()))
                    catalog_filename = configuration->getTypeName() + "://" + configuration->getNamespace() + "/" + metadata_name;

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
