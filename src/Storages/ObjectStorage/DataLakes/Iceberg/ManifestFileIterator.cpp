#include <base/scope_guard.h>
#include "config.h"

#if USE_AVRO

#include <compare>
#include <optional>
#include <unordered_set>

#include <base/arithmeticOverflow.h>

#include <Interpreters/Context.h>
#include <Interpreters/IcebergMetadataLog.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergFieldParseHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <Core/Settings.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Poco/JSON/Parser.h>
#include <Poco/String.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTFunction.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <Common/logger_useful.h>


namespace DB::ErrorCodes
{
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
    extern const int LOGICAL_ERROR;
}

namespace DB::Setting
{
    extern const SettingsBool iceberg_tolerate_conflicting_manifest_schemas;
}

namespace ProfileEvents
{
extern const Event IcebergPartitionPrunedFiles;
extern const Event IcebergMinMaxIndexPrunedFiles;
extern const Event IcebergManifestFileIteratorCreateMicroseconds;
};

namespace DB::Iceberg
{

using namespace DB;

namespace
{
    std::optional<DB::Range> getMaterializedRowLineageRange(
        const ParsedManifestFileEntry & parsed_entry, Int32 field_id, const IcebergPathFromMetadata & path_to_manifest_file)
    {
        auto bounds = parsed_entry.value_bounds.find(field_id);
        if (bounds == parsed_entry.value_bounds.end())
            return std::nullopt;

        auto column_info = parsed_entry.columns_infos.find(field_id);
        if (column_info == parsed_entry.columns_infos.end() || !column_info->second.nulls_count.has_value()
            || *column_info->second.nulls_count != 0)
            return std::nullopt;

        String left_str;
        String right_str;
        if (!bounds->second.first.tryGet(left_str) || !bounds->second.second.tryGet(right_str))
            return std::nullopt;

        auto type = std::make_shared<DB::DataTypeUInt64>();
        auto left = deserializeFieldFromBinaryRepr(left_str, type, true);
        auto right = deserializeFieldFromBinaryRepr(right_str, type, false);
        if (!left || !right)
            return std::nullopt;

        /// An inverted pair is dropped rather than repaired, for the reason spelled out at the guard for
        /// ordinary columns below. Returning nothing here leaves the caller's fallback range, which
        /// bounds every value the file can hold, a materialized one from an earlier write included.
        if (accurateLess(*right, *left))
        {
            LOG_WARNING(
                getLogger("ManifestFileIterator"),
                "Manifest file '{}' declares a lower bound above the upper bound for row lineage column id "
                "{} of data file '{}'; ignoring the declared bounds for this column",
                path_to_manifest_file,
                field_id,
                parsed_entry.file_path_key.serialize());
            return std::nullopt;
        }

        return DB::Range(*left, true, *right, true);
    }

    void addRowLineageHyperrectangles(
        std::unordered_map<Int32, DB::Range> & hyperrectangles,
        const ProcessedManifestFileEntry & entry,
        const IcebergPathFromMetadata & path_to_manifest_file)
    {
        const auto & parsed_entry = *entry.parsed_entry;
        if (!entry.first_row_id.has_value() || parsed_entry.record_count <= 0 || entry.sequence_number < 0)
            return;

        const UInt64 inherited_sequence_number = static_cast<UInt64>(entry.sequence_number);
        /// `first_row_id` and `record_count` are both writer-declared, so the block of row ids they span
        /// need not be representable, and a wrapped last row id would sit below the block's own first one.
        UInt64 last_inherited_row_id = 0;
        if (common::addOverflow<UInt64>(
                *entry.first_row_id, static_cast<UInt64>(parsed_entry.record_count) - 1, last_inherited_row_id))
            return;
        const bool row_ids_are_readable = Poco::toUpper(parsed_entry.file_format) != "ORC";

        for (const auto field_id : {row_id_field_id, last_updated_sequence_number_field_id})
        {
            const bool is_row_id = field_id == row_id_field_id;
            if (is_row_id && !row_ids_are_readable)
                continue;
            const UInt64 inherited_upper_bound = is_row_id ? last_inherited_row_id : inherited_sequence_number;

            if (auto range = getMaterializedRowLineageRange(parsed_entry, field_id, path_to_manifest_file))
            {
                hyperrectangles.emplace(field_id, *range);
                continue;
            }

            /// A missing statistic says nothing about the column, and a materialized value always predates this file, so only the
            /// upper end of the inherited block bounds every value the file can hold.
            hyperrectangles.emplace(field_id, DB::Range(UInt64(0), true, inherited_upper_bound, true));
        }
    }

    /// The bounds of one column of a data file: as declared, and as range borders, where a decimal bound is shifted outward.
    struct ColumnBounds
    {
        DB::Field declared_lower;
        DB::Field declared_upper;
        DB::Field lower;
        DB::Field upper;
    };

    std::optional<ColumnBounds> decodeColumnBounds(
        const ParsedManifestFileEntry & parsed_entry,
        Int32 column_id,
        const DB::DataTypePtr & column_type,
        const IcebergPathFromMetadata & path_to_manifest_file)
    {
        auto bounds_it = parsed_entry.value_bounds.find(column_id);
        if (bounds_it == parsed_entry.value_bounds.end())
            return std::nullopt;
        const auto & bounds = bounds_it->second;

        String left_str;
        String right_str;
        /// lower_bound and upper_bound may be NULL.
        if (!bounds.first.tryGet(left_str) || !bounds.second.tryGet(right_str))
            return std::nullopt;

        if (const auto type_id = column_type->getTypeId();
            type_id == DB::TypeIndex::Tuple || type_id == DB::TypeIndex::Map || type_id == DB::TypeIndex::Array
            || type_id == DB::TypeIndex::Variant)
            return std::nullopt;

        if (DB::removeNullable(column_type)->getTypeId() == DB::TypeIndex::UUID)
        {
            auto point = deserializeUUIDPointFromBinaryRepr(left_str, right_str);
            if (!point)
                return std::nullopt;
            return ColumnBounds{*point, *point, *point, *point};
        }

        auto left = deserializeFieldFromBinaryRepr(left_str, column_type, true);
        auto right = deserializeFieldFromBinaryRepr(right_str, column_type, false);
        if (!left || !right)
        {
            /// A bound that does not decode is wider than the storage of its type, a decimal
            /// bound beyond the precision of its type on the inner side of the range, or
            /// narrower than the column is now, which is what Iceberg leaves behind for a
            /// promoted column. The last is well formed, so this stays out of the warning
            /// log.
            LOG_DEBUG(
                getLogger("ManifestFileIterator"),
                "Manifest file '{}' declares a bound that cannot be read as a usable range border "
                "for column id {} of data file '{}'; ignoring the bounds of this column",
                path_to_manifest_file,
                column_id,
                parsed_entry.file_path_key.serialize());
            return std::nullopt;
        }

        /// At a non-zero scale the outward shift moves each decimal bound one integral unit, so it
        /// un-inverts any declared pair no more than `2 * 10^scale` apart. Only the values as
        /// declared expose that inversion, which is why they are read again here.
        std::optional<DB::Field> declared_left = left;
        std::optional<DB::Field> declared_right = right;
        if (DB::WhichDataType(DB::removeNullable(column_type)).isDecimal())
        {
            declared_left = deserializeFieldFromBinaryRepr(
                left_str, column_type, true, /*compensate_rounding=*/false);
            declared_right = deserializeFieldFromBinaryRepr(
                right_str, column_type, false, /*compensate_rounding=*/false);
        }

        /// A pair inverted as declared means the manifest's statistics are untrustworthy, so no
        /// range derived from them is safe to prune on. Dropping the column's bounds is therefore
        /// right where swapping or clamping them would prune on a value nothing vouches for.
        if (accurateLess(*declared_right, *declared_left))
        {
            LOG_WARNING(
                getLogger("ManifestFileIterator"),
                "Manifest file '{}' declares a lower bound above the upper bound for column id "
                "{} of data file '{}'; ignoring the bounds of this column",
                path_to_manifest_file,
                column_id,
                parsed_entry.file_path_key.serialize());
            return std::nullopt;
        }

        return ColumnBounds{*declared_left, *declared_right, *left, *right};
    }

    /// The fields of the record type of the Avro field `name` in `fields`, nullptr if there is no such record.
    Poco::JSON::Array::Ptr getRecordFields(const Poco::JSON::Array::Ptr & fields, const String & name)
    {
        if (!fields)
            return nullptr;
        for (UInt32 i = 0; i < fields->size(); ++i)
        {
            const auto field = fields->getObject(i);
            if (field && field->optValue<String>(f_name, "") == name)
            {
                const auto type = field->getObject(f_type);
                return type ? type->getArray(f_fields) : nullptr;
            }
        }
        return nullptr;
    }

    /// For a manifest without `partition-spec`, the position in its partition record of the value of each field of
    /// `spec_fields`, matched by field id as the Java reader does, nullopt for a field the record does not have.
    /// Empty if the record is in the order of the spec or has no field ids, so its values are taken by position.
    std::vector<std::optional<size_t>> getPartitionValuePositions(
        const AvroForIcebergDeserializer & deserializer,
        const Poco::JSON::Array::Ptr & spec_fields,
        const IcebergPathFromMetadata & path_to_manifest_file,
        Int32 spec_id,
        size_t & record_fields_count)
    {
        const auto throw_mismatch = [&](std::string_view reason)
        {
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Manifest file '{}' has no 'partition-spec' in its Avro metadata, and its partition values cannot be matched "
                "to the partition spec with id {} of the table metadata: {}",
                path_to_manifest_file,
                spec_id,
                reason);
        };

        Poco::JSON::Array::Ptr record_fields;
        if (auto avro_schema = deserializer.tryGetAvroMetadataValue(f_avro_schema))
        {
            Poco::JSON::Parser parser;
            const auto root = parser.parse(*avro_schema);
            if (root.type() == typeid(Poco::JSON::Object::Ptr))
                record_fields = getRecordFields(
                    getRecordFields(root.extract<Poco::JSON::Object::Ptr>()->getArray(f_fields), f_data_file), f_partition);
        }
        if (!record_fields)
            throw_mismatch("its Avro schema has no 'data_file.partition' record");
        record_fields_count = record_fields->size();

        std::vector<std::optional<Int32>> record_field_ids;
        for (UInt32 i = 0; i < record_fields->size(); ++i)
            record_field_ids.push_back(tryGetInt32Value(record_fields->getObject(i), f_field_id));
        const auto spec_field_ids = getPartitionSpecFieldIds(spec_fields);

        const auto are_complete_and_unique = [](const std::vector<std::optional<Int32>> & ids)
        {
            std::unordered_set<Int32> seen;
            return std::ranges::all_of(ids, [&](const auto & id) { return id.has_value() && seen.insert(*id).second; });
        };
        if (!are_complete_and_unique(spec_field_ids))
            throw_mismatch("the field ids of the partition spec are incomplete or not unique");
        if (std::ranges::none_of(record_field_ids, [](const auto & id) { return id.has_value(); }) && !record_field_ids.empty())
            return {};
        if (!are_complete_and_unique(record_field_ids))
            throw_mismatch("the field ids of the partition record are incomplete or not unique");
        if (record_field_ids.empty() && !spec_field_ids.empty())
            throw_mismatch("the partition record is empty");

        std::vector<std::optional<size_t>> positions(spec_field_ids.size());
        size_t matched_fields = 0;
        bool is_identity = spec_field_ids.size() == record_field_ids.size();
        for (size_t i = 0; i < spec_field_ids.size(); ++i)
        {
            const auto it = std::ranges::find(record_field_ids, spec_field_ids[i]);
            if (it == record_field_ids.end())
            {
                is_identity = false;
                continue;
            }
            positions[i] = static_cast<size_t>(it - record_field_ids.begin());
            is_identity &= *positions[i] == i;
            ++matched_fields;
        }
        if (matched_fields != record_field_ids.size())
            throw_mismatch("the partition record has a field that is not in the partition spec");
        if (is_identity)
            return {};
        return positions;
    }
}

std::unordered_map<Int32, DB::Range> getDataFileHyperrectangles(
    const ProcessedManifestFileEntry & entry,
    const std::unordered_map<Int32, DB::DataTypePtr> & column_types,
    const IcebergPathFromMetadata & path_to_manifest_file)
{
    std::unordered_map<Int32, DB::Range> hyperrectangles;
    const auto & parsed_entry = entry.parsed_entry;
    if (parsed_entry->content_type != FileContentType::DATA)
        return hyperrectangles;

    for (const auto & [column_id, column_type] : column_types)
        if (auto bounds = decodeColumnBounds(*parsed_entry, column_id, column_type, path_to_manifest_file))
            hyperrectangles.emplace(column_id, DB::Range(bounds->lower, true, bounds->upper, true));

    addRowLineageHyperrectangles(hyperrectangles, entry, path_to_manifest_file);
    return hyperrectangles;
}

std::optional<std::pair<DB::Field, DB::Field>> getDataFileColumnBounds(
    const ProcessedManifestFileEntry & entry,
    Int32 column_id,
    const DB::DataTypePtr & column_type,
    const IcebergPathFromMetadata & path_to_manifest_file)
{
    if (entry.parsed_entry->content_type != FileContentType::DATA)
        return std::nullopt;

    auto bounds = decodeColumnBounds(*entry.parsed_entry, column_id, column_type, path_to_manifest_file);
    if (!bounds)
        return std::nullopt;
    return std::pair{std::move(bounds->declared_lower), std::move(bounds->declared_upper)};
}

const std::vector<ProcessedManifestFileEntryPtr> &
ManifestFileIterator::ManifestFileEntriesHandle::getFilesWithoutDeleted(FileContentType content_type) const
{
    switch (content_type)
    {
        case FileContentType::DATA:
            return *data_files;
        case FileContentType::POSITION_DELETE:
            return *position_delete_files;
        case FileContentType::EQUALITY_DELETE:
            return *equality_delete_files;
    }
    UNREACHABLE();
}

bool ManifestFileIterator::ManifestFileEntriesHandle::areAllDataFilesSortedBySortOrderID(Int32 sort_order_id) const
{
    for (const auto & file : *data_files)
    {
        // Treat missing sort_order_id as "not sorted by the expected order".
        // This can happen if:
        // 1. The field is not present in older Iceberg format versions.
        // 2. The data file was written without sort order information.
        if (!file->parsed_entry->sort_order_id.has_value() || (*file->parsed_entry->sort_order_id != sort_order_id))
            return false;
    }
    /// Empty manifest (no data files) is considered sorted by definition
    return true;
}

bool ManifestFileIterator::ManifestFileEntriesHandle::areAllDataFilesEligibleForLazyMaterialization(Int32 table_schema_id) const
{
    /// Equality deletes force reading all physical columns of the data files they apply to
    /// (see IcebergMetadata::getInitialSchemaByPath), so the pruned main read is impossible.
    if (!equality_delete_files->empty())
        return false;

    for (const auto & file : *data_files)
    {
        /// Only Parquet and Vortex readers provide physical row numbers (ChunkInfoRowNumbers)
        /// for the main read and positional re-reads (FormatFilterInfo::rows_to_read)
        /// for the lazy read.
        if (std::string format = Poco::toUpper(file->parsed_entry->file_format); format != "PARQUET" && format != "VORTEX")
            return false;

        /// Schema evolution forces reading all physical columns as well.
        if (file->resolved_schema_id != table_schema_id)
            return false;
    }
    return true;
}

std::optional<UInt64> ManifestFileIterator::ManifestFileEntriesHandle::getRowsCountInAllFilesExcludingDeleted(FileContentType content) const
{
    UInt64 result = 0;
    /// `record_count` is a required file-level field in all format versions, so the sum is
    /// exact: no fallback to optional per-column statistics is needed. The field is parsed
    /// as a raw Int64 though, so a corrupted manifest file may carry a negative value; it
    /// is reported as "count unavailable" rather than summed (a negative contribution would
    /// silently produce a wrong -- or, after the conversion to size_t, absurdly huge --
    /// count) and rather than rejected (the count is only an optimization, a malformed
    /// value must not make the table unreadable).
    for (const auto & file : getFilesWithoutDeleted(content))
    {
        if (file->parsed_entry->record_count < 0)
            return std::nullopt;
        result += static_cast<UInt64>(file->parsed_entry->record_count);
    }
    return result;
}

std::optional<Int64> ManifestFileIterator::ManifestFileEntriesHandle::getBytesCountInAllDataFilesExcludingDeleted() const
{
    size_t result = 0;
    for (const auto & file : getFilesWithoutDeleted(FileContentType::DATA))
    {
        /// Have at least one column with bytes count
        bool found = false;
        for (const auto & [column, column_info] : file->parsed_entry->columns_infos)
        {
            if (column_info.bytes_size.has_value())
            {
                result += *column_info.bytes_size;
                found = true;
                break;
            }
        }

        if (!found)
            return std::nullopt;
    }
    return result;
}

ManifestFileIterator::ManifestFileEntriesHandle ManifestFileIterator::getFilesWithoutDeletedHandle() const
{
    if (!isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get files from manifest file before it is fully initialized");

    SharedLockGuard lock{files_mutex};
    return ManifestFileEntriesHandle
    {
        data_files_without_deleted,
        position_deletes_files_without_deleted,
        equality_deletes_files_without_deleted,
        partition_key_description
    };
}


ManifestFileIterator::~ManifestFileIterator() = default;

std::shared_ptr<ManifestFileIterator> ManifestFileIterator::create(
    std::shared_ptr<AvroForIcebergDeserializer> manifest_file_deserializer_,
    const IcebergPathFromMetadata & path_to_manifest_file_,
    const IcebergPathResolver & path_resolver_,
    IcebergSchemaProcessor & schema_processor,
    Int64 inherited_sequence_number_,
    Int64 inherited_snapshot_id_,
    std::optional<UInt64> inherited_first_row_id_,
    Int32 partition_spec_id_,
    DB::ContextPtr context_,
    std::shared_ptr<const ActionsDAG> filter_dag_,
    Int32 table_snapshot_schema_id_)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::IcebergManifestFileIteratorCreateMicroseconds);

    insertRowToLogTable(
        context_,
        [&] { return manifest_file_deserializer_->getMetadataContent(); },
        DB::IcebergMetadataLogLevel::ManifestFileMetadata,
        path_resolver_.getTableRoot(),
        path_to_manifest_file_,
        std::nullopt,
        std::nullopt);

    /// The manifest file's own format version governs how it is parsed. A v2 table may
    /// still reference v1 manifests produced before an external upgrade from v1 to v2,
    /// and those must remain readable (the Iceberg spec assigns them sequence_number = 0).
    const Int32 manifest_format_version = static_cast<Int32>(manifest_file_deserializer_->getFormatVersionFromManifestFileMetadata());

    for (const auto & column_name : {f_status, f_data_file})
    {
        if (!manifest_file_deserializer_->hasPath(column_name))
            throw Exception(
                DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Required columns are not found in manifest file: {}", column_name);
    }

    Poco::JSON::Parser parser;

    Poco::JSON::Array::Ptr partition_specification;
    std::vector<std::optional<size_t>> partition_value_positions;
    size_t partition_record_fields_count = 0;
    if (auto partition_spec_json_string = manifest_file_deserializer_->tryGetAvroMetadataValue("partition-spec"))
    {
        Poco::Dynamic::Var partition_spec_json = parser.parse(*partition_spec_json_string);
        partition_specification = partition_spec_json.extract<Poco::JSON::Array::Ptr>();
    }
    else
    {
        /// Writers that rewrite manifests with a generic Avro library drop this key. Like the Java reader, take the spec
        /// the manifest list names from the table metadata.
        partition_specification = schema_processor.tryGetPartitionSpecFields(partition_spec_id_);
        if (!partition_specification)
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Manifest file '{}' has no 'partition-spec' in its Avro metadata and the table metadata has no partition spec "
                "with id {}",
                path_to_manifest_file_,
                partition_spec_id_);
        partition_value_positions = getPartitionValuePositions(
            *manifest_file_deserializer_, partition_specification, path_to_manifest_file_, partition_spec_id_, partition_record_fields_count);
    }

    auto schema_json_string = manifest_file_deserializer_->tryGetAvroMetadataValue(f_schema);

    const bool tolerate_conflicting_manifest_schemas = context_->getSettingsRef()[Setting::iceberg_tolerate_conflicting_manifest_schemas];

    std::optional<Int32> header_schema_id;
    if (auto schema_id_string = manifest_file_deserializer_->tryGetAvroMetadataValue(f_schema_id))
        header_schema_id = parse<Int32>(*schema_id_string);

    Int32 manifest_schema_id = 0;
    if (header_schema_id.has_value() && tolerate_conflicting_manifest_schemas
        && schema_processor.isSchemaRegisteredFromMetadata(*header_schema_id))
    {
        manifest_schema_id = *header_schema_id;
    }
    else if (!schema_json_string.has_value())
    {
        /// Without the schema the manifest was written with, take the metadata copy of it: the one the manifest names,
        /// else the schema of the snapshot that added the manifest, else the schema the table is read with.
        if (header_schema_id.has_value())
            manifest_schema_id = *header_schema_id;
        else
            manifest_schema_id = schema_processor.tryGetSchemaIdForSnapshot(inherited_snapshot_id_).value_or(table_snapshot_schema_id_);
        if (!schema_processor.isSchemaRegisteredFromMetadata(manifest_schema_id))
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Manifest file '{}' has no '{}' in its Avro metadata and the table metadata has no schema with id {}",
                path_to_manifest_file_,
                f_schema,
                manifest_schema_id);
    }
    else
    {
        Poco::Dynamic::Var json = parser.parse(*schema_json_string);
        const Poco::JSON::Object::Ptr & schema_object = json.extract<Poco::JSON::Object::Ptr>();
        Int32 embedded_schema_id = schema_object->getValue<int>(f_schema_id);
        if (header_schema_id.has_value() && *header_schema_id != embedded_schema_id)
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Manifest file '{}' has header key '{}' = {} that differs from the '{}' = {} of its '{}' header",
                path_to_manifest_file_,
                f_schema_id,
                *header_schema_id,
                f_schema_id,
                embedded_schema_id,
                f_schema);
        manifest_schema_id = embedded_schema_id;

        schema_processor.addIcebergTableSchema(
            schema_object, IcebergSchemaProcessor::SchemaSource::ManifestFile, tolerate_conflicting_manifest_schemas);
    }

    /// Every entry of this manifest carries one partition value per spec field, including the
    /// fields skipped in buildPartitionKeyFromSpec, so this count is the arity its partition tuples must have.
    const size_t partition_spec_fields_count = partition_specification->size();

    auto partition_key = buildPartitionKeyFromSpec(partition_specification, manifest_schema_id, schema_processor, context_);

    size_t total_rows = manifest_file_deserializer_->rows();

    return std::shared_ptr<ManifestFileIterator>(new ManifestFileIterator(
        std::move(manifest_file_deserializer_),
        path_to_manifest_file_,
        manifest_format_version,
        path_resolver_,
        schema_processor,
        inherited_sequence_number_,
        inherited_snapshot_id_,
        inherited_first_row_id_,
        context_,
        manifest_schema_id,
        std::make_shared<const PartitionSpecification>(std::move(partition_key.partition_specification)),
        std::move(partition_key.key_description),
        partition_spec_fields_count,
        std::move(partition_value_positions),
        partition_record_fields_count,
        total_rows,
        std::move(filter_dag_),
        table_snapshot_schema_id_));
}

ManifestFileIterator::ManifestFileIterator(
    std::shared_ptr<AvroForIcebergDeserializer> manifest_file_deserializer_,
    const IcebergPathFromMetadata & path_to_manifest_file_,
    Int32 format_version_,
    const IcebergPathResolver & path_resolver_,
    IcebergSchemaProcessor & schema_processor,
    Int64 inherited_sequence_number_,
    Int64 inherited_snapshot_id_,
    std::optional<UInt64> inherited_first_row_id_,
    DB::ContextPtr context_,
    Int32 manifest_schema_id_,
    std::shared_ptr<const PartitionSpecification> common_partition_specification_,
    std::optional<DB::KeyDescription> partition_key_description_,
    size_t partition_spec_fields_count_,
    std::vector<std::optional<size_t>> partition_value_positions_,
    size_t partition_record_fields_count_,
    size_t total_rows_,
    std::shared_ptr<const ActionsDAG> filter_dag_,
    Int32 table_snapshot_schema_id_)
    : manifest_file_deserializer(std::move(manifest_file_deserializer_))
    , path_to_manifest_file(path_to_manifest_file_)
    , format_version(format_version_)
    , path_resolver(path_resolver_)
    , inherited_sequence_number(inherited_sequence_number_)
    , inherited_snapshot_id(inherited_snapshot_id_)
    , context(context_)
    , manifest_schema_id(manifest_schema_id_)
    , common_partition_specification(std::move(common_partition_specification_))
    , partition_key_description(
          partition_key_description_ ? std::make_shared<const DB::KeyDescription>(std::move(*partition_key_description_)) : nullptr)
    , partition_spec_fields_count(partition_spec_fields_count_)
    , partition_value_positions(std::move(partition_value_positions_))
    , partition_record_fields_count(partition_record_fields_count_)
    , table_snapshot_schema_id(table_snapshot_schema_id_)
    , total_rows(total_rows_)
    , data_files_without_deleted(std::make_shared<std::vector<ProcessedManifestFileEntryPtr>>())
    , position_deletes_files_without_deleted(std::make_shared<std::vector<ProcessedManifestFileEntryPtr>>())
    , equality_deletes_files_without_deleted(std::make_shared<std::vector<ProcessedManifestFileEntryPtr>>())
    , filter_dag(std::move(filter_dag_))
    , schema_processor_ptr(&schema_processor)
{
    if (!inherited_first_row_id_.has_value())
        return;

    entry_first_row_ids.resize(total_rows);
    UInt64 next_row_id = *inherited_first_row_id_;
    for (size_t row_index = 0; row_index < total_rows; ++row_index)
    {
        const auto parsed_entry = manifest_file_deserializer->getParsedManifestFileEntry(row_index);
        /// Every live data file with a null first_row_id inherits one, EXISTING ones included. A DELETED entry
        /// takes no ids: writers reserve only the added and existing rows of a manifest.
        if (parsed_entry->content_type != FileContentType::DATA || parsed_entry->status == ManifestEntryStatus::DELETED
            || parsed_entry->parsed_first_row_id.has_value())
            continue;

        entry_first_row_ids[row_index] = next_row_id;
        next_row_id += static_cast<UInt64>(parsed_entry->record_count);
    }
}

ProcessedManifestFileEntryPtr ManifestFileIterator::processRow(size_t row_index)
{
    auto parsed_entry = manifest_file_deserializer->getParsedManifestFileEntry(row_index);

    if (!parsed_entry)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Got null pure manifest file entry for row {} in manifest file '{}'",
            row_index,
            path_to_manifest_file);

    if (parsed_entry->status == ManifestEntryStatus::DELETED)
    {
        insertRowToLogTable(
            context,
            [&] { return manifest_file_deserializer->getContent(row_index); },
            DB::IcebergMetadataLogLevel::ManifestFileEntry,
            path_resolver.getTableRoot(),
            path_to_manifest_file,
            row_index,
            std::nullopt);
        return nullptr;
    }

    const DB::Row * partition_key_value = &parsed_entry->partition_key_value;
    DB::Row reordered_partition_key_value;
    if (!partition_value_positions.empty())
    {
        if (partition_key_value->size() != partition_record_fields_count)
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Iceberg manifest partition tuple for file '{}' has {} values but the partition record of manifest file '{}' "
                "has {} fields",
                parsed_entry->file_path_key,
                partition_key_value->size(),
                path_to_manifest_file,
                partition_record_fields_count);
        for (const auto & position : partition_value_positions)
            reordered_partition_key_value.push_back(position ? (*partition_key_value)[*position] : Field());
        partition_key_value = &reordered_partition_key_value;
    }

    /// Iceberg requires one partition value per field of the spec the manifest was written with.
    /// This holds whether or not any of those fields ended up in the partition key.
    if (partition_key_value->size() != partition_spec_fields_count)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Iceberg manifest partition tuple for file '{}' has {} values but the manifest's partition "
            "spec defines {} fields",
            parsed_entry->file_path_key,
            partition_key_value->size(),
            partition_spec_fields_count);

    /// Compute inherited/resolved fields

    Int64 resolved_snapshot_id = 0;
    if (parsed_entry->parsed_snapshot_id.has_value())
    {
        resolved_snapshot_id = *parsed_entry->parsed_snapshot_id;
    }
    else if (parsed_entry->status == ManifestEntryStatus::EXISTING)
    {
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Cannot read Iceberg table: manifest file '{}' has entry with snapshot_id 'null' for which write file schema is unknown",
            path_to_manifest_file);
    }
    else
    {
        resolved_snapshot_id = inherited_snapshot_id;
    }

    const auto schema_id_opt = schema_processor_ptr->tryGetSchemaIdForSnapshot(resolved_snapshot_id);
    if (!schema_id_opt.has_value())
    {
        /// This is expected when the referenced snapshot was expired by the catalog (snapshot expiry is a
        /// normal Iceberg housekeeping operation). For example, after a compaction ("replace" operation),
        /// the new snapshot's manifest list inherits manifests from the now-expired parent snapshot, and
        /// those manifests still carry the original snapshot_id. The manifest file's own Avro header
        /// records the correct schema_id for the data files it describes, so falling back to
        /// manifest_schema_id is safe and correct in this case.
        if (!logged_missing_snapshot_metadata.exchange(true, std::memory_order_relaxed))
            LOG_TEST(
                getLogger("ManifestFileIterator"),
                "Manifest file '{}' has entry with snapshot_id '{}' whose snapshot metadata is not present "
                "(snapshot may have been expired by the catalog). Falling back to manifest schema_id {}.",
                path_to_manifest_file,
                resolved_snapshot_id,
                manifest_schema_id);
    }
    const auto resolved_schema_id = schema_id_opt.has_value() ? *schema_id_opt : manifest_schema_id;

    Int64 resolved_sequence_number = 0;
    if (format_version > 1)
    {
        if (parsed_entry->parsed_sequence_number.has_value())
        {
            resolved_sequence_number = *parsed_entry->parsed_sequence_number;
        }
        else if (parsed_entry->status == ManifestEntryStatus::EXISTING)
        {
            if (!parsed_entry->parsed_sequence_number.has_value())
                throw Exception(
                    DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Data sequence number is null for the file added in another snapshot");
        }
        else
        {
            resolved_sequence_number = inherited_sequence_number;
        }
    }

    auto entry = std::make_shared<ProcessedManifestFileEntry>(
        parsed_entry, common_partition_specification, resolved_sequence_number, resolved_schema_id);

    /// Decode the partition values once, against the schema of the manifest that wrote them, so that
    /// pruning, identity-column projection, delete matching, compaction and `system.iceberg_files` all
    /// see the same value regardless of how this particular manifest encoded it.
    entry->normalized_partition_key_value = normalizePartitionKeyValue(
        *partition_key_value, *common_partition_specification, *schema_processor_ptr, manifest_schema_id);

    if (parsed_entry->parsed_first_row_id.has_value())
        entry->first_row_id = parsed_entry->parsed_first_row_id;
    else if (!entry_first_row_ids.empty())
        entry->first_row_id = entry_first_row_ids[row_index];


    PruningReturnStatus pruning_status = PruningReturnStatus::NOT_PRUNED;
    if (filter_dag)
    {
        const ManifestFilesPruner * current_pruner = getOrCreatePruner(entry->resolved_schema_id);
        const auto hyperrectangles = getDataFileHyperrectangles(*entry, current_pruner->getMinMaxColumnTypes(), path_to_manifest_file);
        pruning_status = current_pruner->canBePruned(entry, hyperrectangles);
    }
    insertRowToLogTable(
        context,
        [&] { return manifest_file_deserializer->getContent(row_index); },
        DB::IcebergMetadataLogLevel::ManifestFileEntry,
        path_resolver.getTableRoot(),
        path_to_manifest_file,
        row_index,
        pruning_status);
    switch (pruning_status)
    {
        case PruningReturnStatus::NOT_PRUNED: {
            std::lock_guard lock(files_mutex);
            switch (entry->parsed_entry->content_type)
            {
                case FileContentType::EQUALITY_DELETE: {
                    equality_deletes_files_without_deleted->emplace_back(entry);
                    return entry;
                }
                case FileContentType::POSITION_DELETE: {
                    position_deletes_files_without_deleted->emplace_back(entry);
                    return entry;
                }
                case FileContentType::DATA: {
                    data_files_without_deleted->emplace_back(entry);
                    return entry;
                }
            }
            UNREACHABLE();
        }
        case PruningReturnStatus::MIN_MAX_INDEX_PRUNED: {
            ProfileEvents::increment(ProfileEvents::IcebergMinMaxIndexPrunedFiles);
            return nullptr;
        }
        case PruningReturnStatus::PARTITION_PRUNED: {
            ProfileEvents::increment(ProfileEvents::IcebergPartitionPrunedFiles);
            return nullptr;
        }
    }
    return entry;
}

const ManifestFilesPruner * ManifestFileIterator::getOrCreatePruner(Int32 schema_id)
{
    std::lock_guard lock(pruners_mutex);
    auto it = pruners_by_schema_id.find(schema_id);
    if (it != pruners_by_schema_id.end())
        return it->second.get();

    auto pruner = std::make_unique<ManifestFilesPruner>(
        *schema_processor_ptr, table_snapshot_schema_id, schema_id, filter_dag.get(), partition_key_description.get(), context);
    return pruners_by_schema_id.emplace(schema_id, std::move(pruner)).first->second.get();
}

bool ManifestFileIterator::isInitialized() const
{
    return fully_initialized && active_fetchers == 0;
}

ProcessedManifestFileEntryPtr ManifestFileIterator::next()
{
    if (fully_initialized.load())
        return nullptr;

    while (true)
    {
        active_fetchers.fetch_add(1);
        SCOPE_EXIT(active_fetchers.fetch_sub(1););
        size_t row_index = current_row_index.fetch_add(1);
        if (row_index >= total_rows)
        {
            fully_initialized.store(true);
            return nullptr;
        }
        auto entry = processRow(row_index);
        if (entry)
            return entry;
    }
}

bool ManifestFileIterator::areAllDataFilesSortedBySortOrderID(Int32 sort_order_id) const
{
    auto handle = getFilesWithoutDeletedHandle();
    for (const auto & file : handle.getFilesWithoutDeleted(FileContentType::DATA))
    {
        // Treat missing sort_order_id as "not sorted by the expected order".
        // This can happen if:
        // 1. The field is not present in older Iceberg format versions.
        // 2. The data file was written without sort order information.
        if (!file->parsed_entry->sort_order_id.has_value() || (*file->parsed_entry->sort_order_id != sort_order_id))
            return false;
    }
    /// Empty manifest (no data files) is considered sorted by definition
    return true;
}

std::optional<Int64> ManifestFileIterator::getBytesCountInAllDataFilesExcludingDeleted() const
{
    Int64 result = 0;
    auto handle = getFilesWithoutDeletedHandle();
    for (const auto & file : handle.getFilesWithoutDeleted(FileContentType::DATA))
    {
        /// Have at least one column with bytes count
        bool found = false;
        for (const auto & [column, column_info] : file->parsed_entry->columns_infos)
        {
            if (column_info.bytes_size.has_value())
            {
                result += *column_info.bytes_size;
                found = true;
                break;
            }
        }

        if (!found)
            return std::nullopt;
    }
    return result;
}

}


#endif
