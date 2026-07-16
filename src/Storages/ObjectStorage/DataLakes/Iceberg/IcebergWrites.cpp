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
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/PODArray_fwd.h>
#include <Common/isValidUTF8.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <cmath>
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
#include <Schema.hh>
#include <Specific.hh>
#include <Stream.hh>
#include <Types.hh>
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
    extern const int DATALAKE_DATABASE_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace FailPoints
{
    extern const char iceberg_writes_cleanup[];
}

static constexpr auto MAX_TRANSACTION_RETRIES = 100;

// NOLINTBEGIN(clang-analyzer-core.uninitialized.UndefReturn)
// Clang analyzer wrongly thinks the avro GenericDatum value can be uninitialized.
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

/// Whether a float/double partition value is NaN, which the manifest-list partition summary records via `contains_nan` rather than as ordered lower/upper bounds.
bool isNaNPartitionValue(const Field & field, DataTypePtr type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::Nullable:
            return !field.isNull()
                && isNaNPartitionValue(field, assert_cast<const DataTypeNullable *>(type.get())->getNestedType());
        case TypeIndex::Float32:
        case TypeIndex::Float64:
            return !field.isNull() && std::isnan(field.safeGet<Float64>());
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
        case TypeIndex::UInt8:
        case TypeIndex::Int8:
        case TypeIndex::UInt16:
        case TypeIndex::Int16:
        case TypeIndex::UInt32:
            return dumpValue(static_cast<Int32>(applyVisitor(FieldVisitorConvertToNumber<Int64>(), field)));
        case TypeIndex::UInt64:
            return dumpValue(applyVisitor(FieldVisitorConvertToNumber<Int64>(), field));
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
    size_t pos = json_str.find("\\/");
    if (pos == String::npos)
        return json_str;

    String result;
    result.reserve(json_str.size());

    size_t start = 0;
    while (pos != String::npos)
    {
        result.append(json_str, start, pos - start);
        result.push_back('/');

        start = pos + 2;
        pos = json_str.find("\\/", start);
    }
    result.append(json_str, start, String::npos);

    return result;
}

String stringifyJSON(const Poco::Dynamic::Var & json, unsigned indent)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(json, oss, indent);
    return removeEscapedSlashes(oss.str());
}

static void extendSchemaForPartitions(
    String & schema,
    const std::vector<String> & partition_columns,
    const DataTypes & partition_types)
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

    std::string json_representation = stringifyJSON(partition_fields);

    std::string from = "#";
    size_t start_pos = schema.find(from);
    if (start_pos != std::string::npos)
    {
        schema.replace(start_pos, from.size(), json_representation);
    }
}

namespace
{
void setVersionedField(avro::GenericRecord & rec, const auto & value, const String & field_name)
{
    size_t field_index = rec.fieldIndex(field_name);
    const avro::NodePtr & field_schema = rec.schema()->leafAt(static_cast<UInt32>(field_index));

    if (field_schema->type() == avro::AVRO_UNION)
    {
        avro::GenericUnion field(field_schema);
        field.selectBranch(1);
        field.datum() = avro::GenericDatum(value);
        rec.fieldAt(field_index) = avro::GenericDatum(field_schema, field);
    }
    else
    {
        rec.fieldAt(field_index) = avro::GenericDatum(value);
    }
}

Poco::JSON::Object::Ptr getCurrentSchema(const Poco::JSON::Object::Ptr & metadata)
{
    Int32 current_schema_id = metadata->getValue<Int32>(Iceberg::f_current_schema_id);
    auto schemas = metadata->getArray(Iceberg::f_schemas);
    for (size_t i = 0; i < schemas->size(); ++i)
    {
        auto schema = schemas->getObject(static_cast<UInt32>(i));
        if (schema->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
            return schema;
    }
    throw Exception(
        ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
        "Not found schema with current-schema-id {} in the schemas list",
        current_schema_id);
}
}

void generateManifestFile(
    Poco::JSON::Object::Ptr metadata,
    const std::vector<String> & partition_columns,
    const std::vector<Field> & partition_values,
    const DataTypes & partition_types,
    const std::vector<IcebergPathFromMetadata> & data_file_names,
    const std::vector<UInt64> & data_file_row_counts,
    const std::vector<UInt64> & data_file_byte_counts,
    const std::optional<DataFileStatistics> & data_file_statistics,
    SharedHeader sample_block,
    Poco::JSON::Object::Ptr new_snapshot,
    const String & format,
    Poco::JSON::Object::Ptr partition_spec,
    Int64 partition_spec_id,
    WriteBuffer & buf,
    Iceberg::FileContentType content_type,
    std::optional<Int64> user_defined_sequence_number,
    const std::vector<String> & data_file_formats,
    const std::vector<DataFileColumnStatistics> & per_file_statistics,
    const std::vector<std::optional<Int32>> & data_file_sort_order_ids,
    const std::vector<DataFileEntryLineage> & per_file_entry_lineage,
    Poco::JSON::Object::Ptr schema_to_serialize)
{
    chassert(
        data_file_formats.empty() || data_file_formats.size() == data_file_names.size(),
        "data_file_formats size does not match number of data files");
    chassert(
        per_file_statistics.empty() || per_file_statistics.size() == data_file_names.size(),
        "per_file_statistics size does not match number of data files");
    chassert(
        data_file_sort_order_ids.empty() || data_file_sort_order_ids.size() == data_file_names.size(),
        "data_file_sort_order_ids size does not match number of data files");
    chassert(
        per_file_entry_lineage.empty() || per_file_entry_lineage.size() == data_file_names.size(),
        "per_file_entry_lineage size does not match number of data files");
    Int32 version = metadata->getValue<Int32>(Iceberg::f_format_version);
    String schema_representation;
    if (version == 1)
        schema_representation = manifest_entry_v1_schema;
    else if (version == 2 || version == 3)
        schema_representation = manifest_entry_v2_schema;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported iceberg format-version {}", version);

    extendSchemaForPartitions(schema_representation, partition_columns, partition_types);
    auto schema = avro::compileJsonSchemaFromString(schema_representation);

    const avro::NodePtr & root_schema = schema.root(); // NOLINT

    if (root_schema->type() != avro::AVRO_RECORD)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg manifest file schema must be record");

    Poco::JSON::Object::Ptr schema_object_to_write = schema_to_serialize ? schema_to_serialize : getCurrentSchema(metadata);
    std::string json_representation = stringifyJSON(schema_object_to_write, 4);

    auto adapter = std::make_unique<OutputStreamWriteBufferAdapter>(buf);
    avro::DataFileWriter<avro::GenericDatum> writer(std::move(adapter), schema);
    writer.setMetadata(Iceberg::f_schema, json_representation);
    writer.setMetadata(Iceberg::f_format_version, std::to_string(version));

    writer.setMetadata(Iceberg::f_partition_spec, stringifyJSON(partition_spec->getArray(Iceberg::f_fields)));
    writer.setMetadata(Iceberg::f_partition_spec_id, std::to_string(partition_spec_id));
    writer.setMetadata(Iceberg::f_format_version, std::to_string(version));
    for (size_t file_idx = 0; file_idx < data_file_names.size(); ++file_idx)
    {
        const auto & data_file_name = data_file_names[file_idx];
        avro::GenericDatum manifest_datum(root_schema);
        avro::GenericRecord & manifest = manifest_datum.value<avro::GenericRecord>();

        /// A metadata-only rewrite (non-empty per_file_entry_lineage) writes each entry as EXISTING, keeping the snapshot-id and sequence number that originally added the file rather than re-stamping it as ADDED.
        const DataFileEntryLineage * entry_lineage
            = per_file_entry_lineage.empty() ? nullptr : &per_file_entry_lineage[file_idx];

        manifest.field(Iceberg::f_status)
            = avro::GenericDatum(entry_lineage ? static_cast<Int32>(ManifestEntryStatus::EXISTING)
                                               : static_cast<Int32>(ManifestEntryStatus::ADDED));
        Int64 snapshot_id = (entry_lineage && entry_lineage->added_snapshot_id)
            ? *entry_lineage->added_snapshot_id
            : new_snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);

        setVersionedField(manifest, snapshot_id, Iceberg::f_snapshot_id);

        if (version > 1)
        {
            Int64 sequence_number = (entry_lineage && entry_lineage->sequence_number)
                ? *entry_lineage->sequence_number
                : user_defined_sequence_number.value_or(new_snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number));

            /// A manifest-only rewrite preserves the source entry's `file_sequence_number`, which can differ from the data
            /// `sequence_number`; for a genuinely new file there is no lineage and it equals the data sequence number.
            Int64 file_sequence_number = (entry_lineage && entry_lineage->file_sequence_number)
                ? *entry_lineage->file_sequence_number
                : sequence_number;

            setVersionedField(manifest, sequence_number, Iceberg::f_sequence_number);
            setVersionedField(manifest, file_sequence_number, Iceberg::f_file_sequence_number);
        }
        avro::GenericRecord & data_file = manifest.field(Iceberg::f_data_file).value<avro::GenericRecord>();
        if (version > 1)
            data_file.field(Iceberg::f_content) = avro::GenericDatum(static_cast<Int32>(content_type));
        data_file.field(Iceberg::f_file_path) = avro::GenericDatum(data_file_name.serialize());
        data_file.field(Iceberg::f_file_format)
            = avro::GenericDatum(data_file_formats.empty() ? format : data_file_formats[file_idx]);

        /// Writes (field-id, value) pairs into the union-typed `field_name` array of the data_file record.
        auto set_fields = [&]<typename K, typename T, typename U>(
                              const std::vector<std::pair<K, T>> & statistics, const std::string & field_name, U && dump_function)
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

        if (!per_file_statistics.empty())
        {
            /// Manifest-only rewrite: carry over the source file's column stats verbatim.
            const auto & stats = per_file_statistics[file_idx];
            /// Bounds are raw bytes; convert to std::vector<uint8_t> to produce an Avro `bytes` datum.
            auto to_bytes = [](Int32, const String & value)
            { return std::vector<uint8_t>(value.begin(), value.end()); };
            set_fields(stats.column_sizes, Iceberg::f_column_sizes, [](Int32, Int64 value) { return value; });
            set_fields(stats.value_counts, Iceberg::f_value_counts, [](Int32, Int64 value) { return value; });
            set_fields(stats.null_value_counts, Iceberg::f_null_value_counts, [](Int32, Int64 value) { return value; });
            set_fields(stats.lower_bounds, Iceberg::f_lower_bounds, to_bytes);
            set_fields(stats.upper_bounds, Iceberg::f_upper_bounds, to_bytes);
        }
        else if (data_file_statistics)
        {
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
            {
                set_fields(lower_statistics, Iceberg::f_lower_bounds, dump_fields);
            }
            auto upper_statistics = data_file_statistics->getUpperBounds();
            if (canWriteStatistics(upper_statistics, field_id_to_column_index, sample_block))
            {
                set_fields(upper_statistics, Iceberg::f_upper_bounds, dump_fields);
            }
        }
        data_file.field(Iceberg::f_record_count) = avro::GenericDatum(static_cast<Int64>(data_file_row_counts[file_idx]));
        data_file.field(Iceberg::f_file_size_in_bytes) = avro::GenericDatum(static_cast<Int64>(data_file_byte_counts[file_idx]));

        /// Preserve the source file's sort_order_id.
        if (!data_file_sort_order_ids.empty() && data_file_sort_order_ids[file_idx].has_value())
        {
            auto & sort_order_field = data_file.field(Iceberg::f_sort_order_id);
            sort_order_field.selectBranch(1);
            sort_order_field.value<Int32>() = *data_file_sort_order_ids[file_idx];
        }

        avro::GenericRecord & partition_record = data_file.field("partition").value<avro::GenericRecord>();
        for (size_t i = 0; i < partition_columns.size(); ++i)
        {
            /// Build the Avro datum holding the partition value; throws on an unsupported type.
            auto make_value_datum = [&]() -> avro::GenericDatum
            {
                switch (partition_values[i].getType())
                {
                    case Field::Types::Int64:
                    case Field::Types::UInt64:
                        return avro::GenericDatum(partition_values[i].safeGet<Int64>());
                    case Field::Types::String:
                        return avro::GenericDatum(partition_values[i].safeGet<String>());
                    case Field::Types::Float64:
                        return avro::GenericDatum(partition_values[i].safeGet<Float64>());
                    case Field::Types::Decimal32:
                        return avro::GenericDatum(partition_values[i].safeGet<Decimal32>().getValue());
                    case Field::Types::Decimal64:
                        return avro::GenericDatum(partition_values[i].safeGet<Decimal64>().getValue());
                    default:
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Unsupported type to write into avro file {}",
                            partition_values[i].getType());
                }
            };

            const bool is_nullable_partition = partition_types[i]->isNullable();
            const bool is_null_value = partition_values[i].getType() == Field::Types::Null;

            if (is_nullable_partition)
            {
                /// Nullable partition columns are Avro `["null", T]` unions: NULL is branch 0, a value is branch 1.
                size_t field_index = 0;
                if (!partition_record.schema()->nameIndex(partition_columns[i], field_index))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Partition field {} not found in manifest schema",
                        partition_columns[i]);

                const avro::NodePtr & union_schema = partition_record.schema()->leafAt(static_cast<UInt32>(field_index));

                avro::GenericUnion union_field(union_schema);
                if (is_null_value)
                {
                    union_field.selectBranch(0);
                }
                else
                {
                    union_field.selectBranch(1);
                    union_field.datum() = make_value_datum();
                }
                partition_record.field(partition_columns[i]) = avro::GenericDatum(union_schema, union_field);
            }
            else
            {
                if (is_null_value)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Got NULL partition value for non-nullable partition column {}",
                        partition_columns[i]);
                partition_record.field(partition_columns[i]) = make_value_datum();
            }
        }

        writer.write(manifest_datum);
    }
    writer.close();
}

void generateManifestList(
    const Iceberg::IcebergPathResolver & path_resolver,
    Poco::JSON::Object::Ptr metadata,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    const std::vector<Iceberg::IcebergPathFromMetadata> & manifest_entry_names,
    Poco::JSON::Object::Ptr new_snapshot,
    const std::vector<Int64> & manifest_entry_sizes,
    WriteBuffer & buf,
    Iceberg::FileContentType content_type,
    bool use_previous_snapshots,
    const std::vector<Iceberg::FileContentType> & per_entry_content_types,
    const std::vector<ManifestListEntryExistingCounts> & existing_entry_counts,
    const std::unordered_set<String> & carry_forward_manifest_paths,
    const std::vector<Int64> & entry_partition_spec_ids,
    const std::vector<std::vector<std::pair<Field, DataTypePtr>>> & entry_partition_summaries)
{
    chassert(
        per_entry_content_types.empty() || per_entry_content_types.size() == manifest_entry_names.size(),
        "per_entry_content_types size does not match number of manifest entries");
    chassert(
        entry_partition_spec_ids.empty() || entry_partition_spec_ids.size() == manifest_entry_names.size(),
        "entry_partition_spec_ids size does not match number of manifest entries");
    chassert(
        entry_partition_summaries.empty() || entry_partition_summaries.size() == manifest_entry_names.size(),
        "entry_partition_summaries size does not match number of manifest entries");
    /// When provided, existing_entry_counts marks a manifest-only rewrite and supplies per-entry counts.
    chassert(
        existing_entry_counts.empty() || existing_entry_counts.size() == manifest_entry_names.size(),
        "existing_entry_counts size does not match number of manifest entries");
    const bool manifest_only_rewrite = !existing_entry_counts.empty();

    Int32 version = metadata->getValue<Int32>(Iceberg::f_format_version);
    String schema_representation;
    if (version == 1)
        schema_representation = manifest_list_v1_schema;
    else
        schema_representation = manifest_list_v2_schema;

    auto schema = avro::compileJsonSchemaFromString(schema_representation); // NOLINT

    auto adapter = std::make_unique<OutputStreamWriteBufferAdapter>(buf);
    avro::DataFileWriter<avro::GenericDatum> writer(std::move(adapter), schema);
    writer.setMetadata(Iceberg::f_format_version, std::to_string(version));

    for (size_t entry_idx = 0; entry_idx < manifest_entry_names.size(); ++entry_idx)
    {
        avro::GenericDatum entry_datum(schema.root());
        avro::GenericRecord & entry = entry_datum.value<avro::GenericRecord>();

        const Iceberg::FileContentType entry_content
            = per_entry_content_types.empty() ? content_type : per_entry_content_types[entry_idx];

        entry.field(Iceberg::f_manifest_path) = manifest_entry_names[entry_idx].serialize();
        entry.field(Iceberg::f_manifest_length) = manifest_entry_sizes[entry_idx];
        entry.field(Iceberg::f_partition_spec_id) = entry_partition_spec_ids.empty()
            ? metadata->getValue<Int64>(Iceberg::f_default_spec_id)
            : entry_partition_spec_ids[entry_idx];
        if (version > 1)
        {
            entry.field(Iceberg::f_content) = static_cast<Int32>(entry_content);
            /// For a manifest-only rewrite, min_sequence_number is the per-manifest minimum of the preserved original sequence numbers.
            const Int64 new_sequence_number = new_snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number);
            entry.field(Iceberg::f_sequence_number) = new_sequence_number;
            entry.field(Iceberg::f_min_sequence_number)
                = manifest_only_rewrite ? existing_entry_counts[entry_idx].min_sequence_number : new_sequence_number;
        }

        entry.field(Iceberg::f_added_snapshot_id) = new_snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        auto summary = new_snapshot->getObject(Iceberg::f_summary);
        if (manifest_only_rewrite)
        {
            /// Manifest-only rewrite (`replace`): data files already existed, so they are reported as existing, not added.
            const auto & counts = existing_entry_counts[entry_idx];
            setVersionedField(entry, 0, Iceberg::f_added_files_count);
            setVersionedField(entry, counts.existing_files_count, Iceberg::f_existing_files_count);
            setVersionedField(entry, 0, Iceberg::f_deleted_files_count);
            setVersionedField(entry, 0, Iceberg::f_added_rows_count);
            setVersionedField(entry, counts.existing_rows_count, Iceberg::f_existing_rows_count);
            setVersionedField(entry, 0, Iceberg::f_deleted_rows_count);

            /// Recompute the `partitions` summary so pruning bounds survive the rewrite (lower_bound == upper_bound per field).
            if (!entry_partition_summaries.empty())
            {
                auto & partitions_field = entry.field(Iceberg::f_partitions);
                partitions_field.selectBranch(1);
                auto & summaries = partitions_field.value<avro::GenericArray>();
                auto summary_schema = summaries.schema()->leafAt(0);
                for (const auto & [partition_value, partition_type] : entry_partition_summaries[entry_idx])
                {
                    avro::GenericDatum summary_datum(summary_schema);
                    auto & summary_record = summary_datum.value<avro::GenericRecord>();
                    const bool is_null = partition_value.isNull();
                    summary_record.field(Iceberg::f_contains_null) = avro::GenericDatum(is_null);
                    if (!is_null)
                    {
                        if (isNaNPartitionValue(partition_value, partition_type))
                        {
                            /// NaN float/double partition value: record it via `contains_nan` instead of publishing the NaN bytes as ordered bounds.
                            auto & contains_nan = summary_record.field(Iceberg::f_contains_nan);
                            contains_nan.selectBranch(1);
                            contains_nan.value<bool>() = true;
                        }
                        else if (canDumpIcebergStats(partition_value, partition_type))
                        {
                            auto bound = dumpFieldToBytes(partition_value, partition_type);
                            auto & lower = summary_record.field(Iceberg::f_lower_bound);
                            lower.selectBranch(1);
                            lower.value<std::vector<uint8_t>>() = bound;
                            auto & upper = summary_record.field(Iceberg::f_upper_bound);
                            upper.selectBranch(1);
                            upper.value<std::vector<uint8_t>>() = bound;
                        }
                        /// else: a partition type whose bounds we cannot serialize (e.g. Decimal); leave the bounds null, matching the data-file statistics path.
                    }
                    summaries.value().push_back(summary_datum);
                }
            }

            writer.write(entry_datum);
            continue;
        }

        if (version == 1)
        {
            setVersionedField(entry, 1, Iceberg::f_added_files_count);
            setVersionedField(entry, std::stoi(summary->getValue<String>(Iceberg::f_total_data_files)), Iceberg::f_existing_files_count);
            setVersionedField(entry, 0, Iceberg::f_deleted_files_count);
            if (summary->has(Iceberg::f_added_position_deletes))
                setVersionedField(entry, summary->getValue<Int64>(Iceberg::f_added_position_deletes), Iceberg::f_deleted_rows_count);
            else
                setVersionedField(entry, 0, Iceberg::f_deleted_rows_count);
        }
        else
        {
            entry.field(Iceberg::f_added_files_count) = 1;
            /// This manifest only contains newly added files; no pre-existing entries.
            entry.field(Iceberg::f_existing_files_count) = 0;
            entry.field(Iceberg::f_deleted_files_count) = 0;
            if (summary->has(Iceberg::f_added_position_deletes))
                entry.field(Iceberg::f_deleted_rows_count) = summary->getValue<Int64>(Iceberg::f_added_position_deletes);
            else
                entry.field(Iceberg::f_deleted_rows_count) = 0;
        }

        if (entry_content == Iceberg::FileContentType::DATA)
        {
            setVersionedField(
                entry,
                summary->has(Iceberg::f_added_records) ? summary->getValue<Int64>(Iceberg::f_added_records) : 0,
                Iceberg::f_added_rows_count);
        }
        else
        {
            setVersionedField(
                entry,
                summary->has(Iceberg::f_added_position_deletes) ? summary->getValue<Int64>(Iceberg::f_added_position_deletes) : 0,
                Iceberg::f_added_rows_count);
        }
        setVersionedField(
            entry,
            0,
            Iceberg::f_existing_rows_count);
        setVersionedField(entry, 0, Iceberg::f_deleted_rows_count);

        writer.write(entry_datum);
    }

    /// Copy entries from the parent snapshot's manifest list: `use_previous_snapshots` copies all, `carry_forward_manifest_paths` copies only the listed manifests.
    if (use_previous_snapshots || !carry_forward_manifest_paths.empty())
    {
        auto parent_snapshot_id = new_snapshot->getValue<Int64>(Iceberg::f_parent_snapshot_id);
        auto snapshots = metadata->getArray(Iceberg::f_snapshots);
        for (size_t i = 0; i < snapshots->size(); ++i)
        {
            if (snapshots->getObject(static_cast<UInt32>(i))->getValue<Int64>(Iceberg::f_metadata_snapshot_id) == parent_snapshot_id)
            {
                auto manifest_list = Iceberg::IcebergPathFromMetadata::deserialize(
                    snapshots->getObject(static_cast<UInt32>(i))->getValue<String>(Iceberg::f_manifest_list));

                auto resolved_manifest_list_path = path_resolver.resolve(manifest_list);
                forEachAvroEntry(resolved_manifest_list_path, object_storage, context, "IcebergWrites",
                    [&](const avro::GenericDatum & datum)
                    {
                        const avro::GenericRecord & old_entry = datum.value<avro::GenericRecord>();
                        /// When a path filter is supplied, copy only the matching entries.
                        if (!carry_forward_manifest_paths.empty()
                            && !carry_forward_manifest_paths.contains(old_entry.field(Iceberg::f_manifest_path).value<std::string>()))
                            return;
                        avro::GenericDatum new_datum(schema.root());
                        avro::GenericRecord & new_entry = new_datum.value<avro::GenericRecord>();
                        new_entry.field(f_manifest_path) = old_entry.field(Iceberg::f_manifest_path);
                        new_entry.field(f_manifest_length) = old_entry.field(Iceberg::f_manifest_length);
                        new_entry.field(f_partition_spec_id) = old_entry.field(Iceberg::f_partition_spec_id);
                        /// iceberg-spark changed `f_added_snapshot_id` from 'null, long' to 'long' (apache/iceberg#11626); rewrite with the new schema in case we read the old type.
                        if (old_entry.hasField(Iceberg::f_added_snapshot_id))
                        {
                            const avro::GenericDatum & old_added_snapshot_id_entry = old_entry.field(Iceberg::f_added_snapshot_id);
                            if (old_added_snapshot_id_entry.isUnion())
                            {
                                if (old_added_snapshot_id_entry.unionBranch() == 0) /// it means add_snapshot_id is null
                                {
                                    /// This only happens when we read data written by a old version of iceberg, which violates the spec of iceberg.
                                    throw Exception(
                                        ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                                        "Manifest list {} has null value for field '{}', but it is required",
                                        resolved_manifest_list_path,
                                        Iceberg::f_added_snapshot_id);
                                }
                            }
                            new_entry.field(f_added_snapshot_id) = old_added_snapshot_id_entry.value<Int64>();
                        }
                        else
                            /// This only happens when we read data written by a old version of iceberg, which violates the spec of iceberg.
                            throw Exception(
                                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                                "Manifest list {} has null value for field '{}', but it is required",
                                resolved_manifest_list_path,
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
                        /// v2 and v3 share the manifest-list schema, so these fields exist for both.
                        if (version > 1)
                        {
                            add_field_to_datum(Iceberg::f_content);
                            add_field_to_datum(Iceberg::f_sequence_number);
                            add_field_to_datum(Iceberg::f_min_sequence_number);
                        }
                        writer.write(new_datum);
                    });
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
{
    auto [last_version, metadata_path, compression_method] = getLatestMetadataFileAndVersionWithCatalog(
        object_storage,
        catalog,
        table_id.getTableName(),
        persistent_table_components.table_path,
        data_lake_settings,
        persistent_table_components.metadata_cache,
        context_,
        log.get(),
        persistent_table_components.table_uuid,
        persistent_table_components.metadata_compression_method,
        /* ignore_explicit_metadata_file_path */ false);

    metadata = getMetadataJSONObject(
        metadata_path,
        object_storage,
        persistent_table_components.metadata_cache,
        context,
        log,
        compression_method,
        persistent_table_components.table_uuid);
    metadata_compression_method = compression_method;
    filename_generator = FileNamesGenerator(
        persistent_table_components.path_resolver.getTableLocation(),
        (catalog != nullptr && catalog->isTransactional()), metadata_compression_method, write_format);

    filename_generator.setVersion(last_version + 1);

    if (metadata->has(Iceberg::f_properties))
    {
        auto properties = metadata->getObject(Iceberg::f_properties);
        if (properties && properties->has("write.data.path"))
            filename_generator.setDataLocation(properties->getValue<String>("write.data.path"));
    }

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
                partitioner = ChunkPartitioner(current_partition_spec->getArray(Iceberg::f_fields), current_schema->getArray(Iceberg::f_fields), context_, std::make_shared<const Block>(extended_block_for_sorting));
            break;
        }
    }
}

IcebergStorageSink::~IcebergStorageSink()
{
    cancelBuffers();
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
                persistent_table_components.path_resolver,
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
    {
        cancelBuffers();
        return;
    }

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

    /// TODO: there's a chance that initializeMetadata() doesn't succeed within MAX_TRANSACTION_RETRIES without throwing, perhaps we should fail in this case
    size_t i = 0;
    bool successed_write = false;
    while (i < MAX_TRANSACTION_RETRIES)
    {
        if (initializeMetadata())
        {
            successed_write = true;
            break;
        }
        ++i;
    }
    if (!successed_write)
        throw Exception(ErrorCodes::DATALAKE_DATABASE_ERROR, "Write into iceberg was not successful");
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
    const auto & resolver = persistent_table_components.path_resolver;
    auto metadata_info = filename_generator.generateMetadataPathWithInfo();
    auto hint_path = filename_generator.generateVersionHint();

    Int64 parent_snapshot = -1;
    if (metadata->has(Iceberg::f_current_snapshot_id) && !metadata->isNull(Iceberg::f_current_snapshot_id))
        parent_snapshot = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);

    Int64 total_data_files = 0;
    for (const auto & [_, writer] : writer_per_partition_key)
        total_data_files += static_cast<Int64>(writer.getDataFiles().size());
    auto [new_snapshot, manifest_list_path] = MetadataGenerator(metadata).generateNextMetadata(
        filename_generator,
        metadata_info.path,
        parent_snapshot,
        total_data_files,
        total_rows,
        total_chunks_size,
        /* num_partitions */ static_cast<Int64>(writer_per_partition_key.size()),
        /* added_delete_files */ 0,
        /* num_deleted_rows */ 0);
    auto storage_manifest_list_name = resolver.resolve(manifest_list_path);


    Strings manifest_entries_in_storage;
    std::vector<Iceberg::IcebergPathFromMetadata> manifest_entries;
    std::vector<Int64> manifest_entry_sizes;

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
            /// When retrying after a metadata conflict, we must read the actual latest
            /// metadata version, not the explicitly specified one. If a table was created
            /// with iceberg_metadata_file_path (e.g. for time-travel reads), the retry
            /// loop must still discover the real latest version to advance past it.
            /// Otherwise the loop keeps regenerating the same target version and fails.
            auto [last_version, metadata_path, compression_method] = getLatestMetadataFileAndVersionWithCatalog(
                object_storage,
                catalog,
                table_id.getTableName(),
                persistent_table_components.table_path,
                data_lake_settings,
                persistent_table_components.metadata_cache,
                context,
                getLogger("IcebergWrites").get(),
                persistent_table_components.table_uuid,
                persistent_table_components.metadata_compression_method,
                /* ignore_explicit_metadata_file_path */ true);

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
                        partitioner = ChunkPartitioner(current_partition_spec->getArray(Iceberg::f_fields), current_schema->getArray(Iceberg::f_fields), context, sample_block);
                    break;
                }
            }
        }
    };

    try
    {
        for (const auto & [partition_key, writer] : writer_per_partition_key)
        {
            auto manifest_entry_path = filename_generator.generateManifestEntryName();
            manifest_entries_in_storage.push_back(resolver.resolve(manifest_entry_path));
            manifest_entries.push_back(manifest_entry_path);

            auto buffer_manifest_entry = object_storage->writeObject(
                StoredObject(resolver.resolve(manifest_entry_path)), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
            try
            {
                generateManifestFile(
                    metadata,
                    partitioner ? partitioner->getColumns() : std::vector<String>{},
                    partition_key,
                    partitioner ? partitioner->getResultTypes() : DataTypes{},
                    writer.getDataFiles(),
                    writer.getDataFileRowCounts(),
                    writer.getDataFileByteCounts(),
                    writer.getResultStatistics(),
                    sample_block,
                    new_snapshot,
                    write_format,
                    partititon_spec,
                    partition_spec_id,
                    *buffer_manifest_entry,
                    Iceberg::FileContentType::DATA);
                buffer_manifest_entry->finalize();
                auto size = buffer_manifest_entry->count();
                if (size == 0)
                {
                    size = object_storage->getObjectMetadata(resolver.resolve(manifest_entry_path), /*with_tags=*/false).size_bytes;
                }
                manifest_entry_sizes.push_back(size);
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
                    persistent_table_components.path_resolver,
                    metadata, object_storage, context,
                    manifest_entries,
                    new_snapshot,
                    manifest_entry_sizes,
                    *buffer_manifest_list,
                    Iceberg::FileContentType::DATA,
                    /* use_previous_snapshots = */ true);
                buffer_manifest_list->finalize();
            }
            catch (...)
            {
                cleanup(false);
                throw;
            }
        }

        {
            std::string json_representation = stringifyJSON(metadata, 4);

            fiu_do_on(FailPoints::iceberg_writes_cleanup,
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failpoint for cleanup enabled");
            });

            LOG_DEBUG(log, "Writing new metadata file {}", metadata_info.path);
            const bool catalog_writes_metadata_file = catalog && catalog->isTransactional();
            if (!catalog_writes_metadata_file)
            {
                if (!writeMetadataFileAndVersionHint(
                        persistent_table_components.path_resolver,
                        metadata_info,
                        json_representation,
                        hint_path,
                        object_storage,
                        context,
                        data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
                {
                    LOG_DEBUG(log, "Failed to write metadata {}, retrying", metadata_info.path);
                    cleanup(true);
                    return false;
                }
                LOG_DEBUG(log, "Metadata file {} written", metadata_info.path);
            }

            if (catalog)
            {
                auto catalog_filename = resolver.resolveForCatalog(metadata_info.path);

                const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());
                if (!catalog->updateMetadata(namespace_name, table_name, catalog_filename, new_snapshot))
                {
                    cleanup(true);
                    return false;
                }
            }
        }

        /// Invalidate the cache so the next reader gets the latest version, which a concurrent catalog update may have changed.
        persistent_table_components.invalidateMetadataCache();
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
