#include <base/scope_guard.h>
#include "config.h"

#if USE_AVRO

#include <compare>
#include <optional>
#include <unordered_set>

#include <base/arithmeticOverflow.h>

#include <Interpreters/IcebergMetadataLog.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <Core/TypeId.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Poco/JSON/Parser.h>
#include <Poco/String.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTFunction.h>
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
    extern const int BAD_ARGUMENTS;
}

namespace ProfileEvents
{
extern const Event IcebergPartitionPrunedFiles;
extern const Event IcebergMinMaxIndexPrunedFiles;
};

namespace DB::Iceberg
{

using namespace DB;

namespace
{
    /// Iceberg store decimal values as unscaled value with two's-complement big-endian binary
    /// using the minimum number of bytes for the value
    /// Our decimal binary representation is little endian
    /// so we cannot reuse our default code for parsing it.
    ///
    /// NOTE: It's very weird, but Decimal values for lower bound and upper bound
    /// are stored rounded, without fractional part. What is more strange
    /// the integer part is rounded mathematically correctly according to fractional part.
    /// Example: 17.22 -> 17, 8888.999 -> 8889, 1423.77 -> 1424.
    /// I've checked two implementations: Spark and Amazon Athena and both of them
    /// do this.
    ///
    /// The problem is -- we cannot use rounded values for lower bounds and upper bounds.
    /// Example: upper_bound(x) = 17.22, but it's rounded 17.00, now condition WHERE x >= 17.21 will
    /// check rounded value and say: "Oh largest value is 17, so values bigger than 17.21 cannot be in this file,
    /// let's skip it". But it will produce incorrect result since actual value (17.22 >= 17.21) is stored in this file.
    ///
    /// To handle this issue we subtract 1 from the integral part for lower_bound and add 1 to integral
    /// part of upper_bound. This produces: 17.22 -> [16.0, 18.0]. So this is more rough boundary,
    /// but at least it doesn't lead to incorrect results.
    /// `compensate_rounding` widens the bound as described above; pass false to read the value exactly
    /// as the manifest declares it.
    template <typename DecimalType>
    std::optional<DB::Field>
    deserializeDecimalBound(const std::string & str, UInt32 scale, bool lower_bound, bool compensate_rounding = true)
    {
        using NativeType = typename DecimalType::NativeType;
        using UnsignedType = make_unsigned_t<NativeType>;

        if (str.size() > sizeof(NativeType))
            return std::nullopt;

        /// Accumulate into the unsigned counterpart, pre-filled with the sign bits,
        /// so that the sign extension comes out of the shifts themselves.
        UnsignedType unscaled = (str[0] & 0x80) ? ~UnsignedType(0) : UnsignedType(0);
        for (const auto byte : str)
            unscaled = (unscaled << 8) | static_cast<UInt8>(byte);

        NativeType unscaled_value = static_cast<NativeType>(unscaled);

        if (compensate_rounding && scale)
        {
            NativeType scaler = lower_bound ? -10 : 10;
            for (UInt32 i = 1; i < scale; ++i)
                scaler *= 10;

            /// The bound is stored as raw bytes and is never checked against the declared precision, so
            /// widening it can leave the type. A value that has no widened form is not a usable bound.
            if (common::addOverflow(unscaled_value, scaler, unscaled_value))
                return std::nullopt;
        }

        return DB::DecimalField<DecimalType>(unscaled_value, scale);
    }

    /// Iceberg stores lower_bounds and upper_bounds serialized with some custom deserialization as bytes array
    /// https://iceberg.apache.org/spec/#appendix-d-single-value-serialization
    std::optional<DB::Field> deserializeFieldFromBinaryRepr(
        std::string str, DB::DataTypePtr expected_type, bool lower_bound, bool compensate_rounding = true)
    {
        auto non_nullable_type = DB::removeNullable(expected_type);
        auto column = non_nullable_type->createColumn();
        if (DB::WhichDataType(non_nullable_type).isDecimal())
        {
            if (str.empty())
                return std::nullopt;

            const UInt32 scale = DB::getDecimalScale(*non_nullable_type);
            if (DB::checkDecimal<DB::Decimal32>(*non_nullable_type))
                return deserializeDecimalBound<DB::Decimal32>(str, scale, lower_bound, compensate_rounding);
            if (DB::checkDecimal<DB::Decimal64>(*non_nullable_type))
                return deserializeDecimalBound<DB::Decimal64>(str, scale, lower_bound, compensate_rounding);
            if (DB::checkDecimal<DB::Decimal128>(*non_nullable_type))
                return deserializeDecimalBound<DB::Decimal128>(str, scale, lower_bound, compensate_rounding);
            if (DB::checkDecimal<DB::Decimal256>(*non_nullable_type))
                return deserializeDecimalBound<DB::Decimal256>(str, scale, lower_bound, compensate_rounding);
            return std::nullopt;
        }
        else if (non_nullable_type->getTypeId() == DB::TypeIndex::Variant)
        {
            return std::nullopt;
        }
        else
        {
            /// For all other types except decimal binary representation
            /// matches our internal representation
            column->insertData(str.data(), str.length());
            DB::Field result;
            column->get(0, result);
            return result;
        }
    }

}

namespace
{
    std::optional<DB::Range> getMaterializedRowLineageRange(const ParsedManifestFileEntry & parsed_entry, Int32 field_id)
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

        return DB::Range(*left, true, *right, true);
    }

    bool isColumnPresenceKnown(const ParsedManifestFileEntry & parsed_entry)
    {
        for (const auto & [field_id, column_info] : parsed_entry.columns_infos)
            if (column_info.bytes_size.has_value())
                return true;
        return false;
    }

    void addRowLineageHyperrectangles(std::unordered_map<Int32, DB::Range> & hyperrectangles, const ProcessedManifestFileEntry & entry)
    {
        const auto & parsed_entry = *entry.parsed_entry;
        if (!entry.first_row_id.has_value() || parsed_entry.record_count <= 0 || entry.sequence_number < 0)
            return;

        const UInt64 inherited_sequence_number = static_cast<UInt64>(entry.sequence_number);
        const UInt64 last_inherited_row_id = *entry.first_row_id + static_cast<UInt64>(parsed_entry.record_count) - 1;
        const bool column_presence_is_known = isColumnPresenceKnown(parsed_entry);
        const bool row_ids_are_readable = Poco::toUpper(parsed_entry.file_format) != "ORC";

        for (const auto field_id : {row_id_field_id, last_updated_sequence_number_field_id})
        {
            const bool is_row_id = field_id == row_id_field_id;
            if (is_row_id && !row_ids_are_readable)
                continue;
            const UInt64 inherited_lower_bound = is_row_id ? *entry.first_row_id : inherited_sequence_number;
            const UInt64 inherited_upper_bound = is_row_id ? last_inherited_row_id : inherited_sequence_number;

            if (!parsed_entry.columns_infos.contains(field_id))
            {
                if (column_presence_is_known)
                {
                    hyperrectangles.emplace(field_id, DB::Range(inherited_lower_bound, true, inherited_upper_bound, true));
                    continue;
                }
            }
            else if (auto range = getMaterializedRowLineageRange(parsed_entry, field_id))
            {
                hyperrectangles.emplace(field_id, *range);
                continue;
            }

            hyperrectangles.emplace(field_id, DB::Range(UInt64(0), true, inherited_upper_bound, true));
        }
    }
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
        /// Only the Parquet reader provides physical row numbers (ChunkInfoRowNumbers)
        /// for the main read and positional re-reads (FormatFilterInfo::rows_to_read)
        /// for the lazy read.
        if (Poco::toUpper(file->parsed_entry->file_format) != "PARQUET")
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
        equality_deletes_files_without_deleted
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
    DB::ContextPtr context_,
    std::shared_ptr<const ActionsDAG> filter_dag_,
    Int32 table_snapshot_schema_id_,
    const std::atomic<bool> * stop_flag_)
{
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

    auto partition_spec_json_string = manifest_file_deserializer_->tryGetAvroMetadataValue("partition-spec");
    if (!partition_spec_json_string.has_value())
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "No partition-spec in iceberg manifest file");

    Poco::Dynamic::Var partition_spec_json = parser.parse(*partition_spec_json_string);
    const Poco::JSON::Array::Ptr & partition_specification = partition_spec_json.extract<Poco::JSON::Array::Ptr>();

    DB::NamesAndTypesList partition_columns_description;
    std::unordered_set<String> partition_columns_seen;
    auto partition_key_ast = make_intrusive<ASTFunction>();
    partition_key_ast->name = "tuple";
    partition_key_ast->arguments = make_intrusive<DB::ASTExpressionList>();
    partition_key_ast->children.push_back(partition_key_ast->arguments);

    auto schema_json_string = manifest_file_deserializer_->tryGetAvroMetadataValue(f_schema);
    if (!schema_json_string.has_value())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot read Iceberg table: manifest file '{}' doesn't have field '{}' in its metadata",
            path_to_manifest_file_,
            f_schema);

    Poco::Dynamic::Var json = parser.parse(*schema_json_string);
    const Poco::JSON::Object::Ptr & schema_object = json.extract<Poco::JSON::Object::Ptr>();
    Int32 manifest_schema_id = schema_object->getValue<int>(f_schema_id);

    schema_processor.addIcebergTableSchema(schema_object);

    /// Every entry of this manifest carries one partition value per spec field, including the
    /// fields skipped below, so this count is the arity its partition tuples must have.
    const size_t partition_spec_fields_count = partition_specification->size();

    PartitionSpecification partition_spec_vec;
    for (size_t i = 0; i != partition_specification->size(); ++i)
    {
        auto partition_specification_field = partition_specification->getObject(static_cast<UInt32>(i));

        auto source_id = partition_specification_field->getValue<Int32>(f_source_id);
        /// NOTE: tricky part to support RENAME column in partition key. Instead of some name
        /// we use column internal number as it's name.
        auto numeric_column_name = DB::backQuote(DB::toString(source_id));
        std::optional<DB::NameAndTypePair> manifest_file_column_characteristics
            = schema_processor.tryGetFieldCharacteristics(manifest_schema_id, source_id);
        if (!manifest_file_column_characteristics.has_value())
            continue;
        auto transform_name = partition_specification_field->getValue<String>(f_partition_transform);
        auto partition_name = partition_specification_field->getValue<String>(f_partition_name);
        partition_spec_vec.emplace_back(source_id, transform_name, partition_name, static_cast<Int32>(i));
        auto partition_ast = getASTFromTransform(transform_name, numeric_column_name);
        /// Unsupported partition key expression
        if (partition_ast == nullptr)
            continue;

        partition_key_ast->as<ASTFunction>()->arguments->children.emplace_back(std::move(partition_ast));
        /// One source column may back several partition fields (e.g. hours(ts) and identity ts).
        /// The tuple key AST keeps one child per field, but getKeyFromAST resolves identifiers
        /// against these input columns, which must contain each source column at most once.
        if (partition_columns_seen.insert(numeric_column_name).second)
            partition_columns_description.emplace_back(numeric_column_name, removeNullable(manifest_file_column_characteristics->type));
    }

    std::optional<DB::KeyDescription> partition_key_description;
    if (!partition_columns_description.empty())
        partition_key_description.emplace(
            DB::KeyDescription::getKeyFromAST(std::move(partition_key_ast), ColumnsDescription(partition_columns_description), {}, context_));

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
        std::make_shared<const PartitionSpecification>(std::move(partition_spec_vec)),
        std::move(partition_key_description),
        partition_spec_fields_count,
        total_rows,
        std::move(filter_dag_),
        table_snapshot_schema_id_,
        stop_flag_));
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
    size_t total_rows_,
    std::shared_ptr<const ActionsDAG> filter_dag_,
    Int32 table_snapshot_schema_id_,
    const std::atomic<bool> * stop_flag_)
    : manifest_file_deserializer(std::move(manifest_file_deserializer_))
    , path_to_manifest_file(path_to_manifest_file_)
    , format_version(format_version_)
    , path_resolver(path_resolver_)
    , inherited_sequence_number(inherited_sequence_number_)
    , inherited_snapshot_id(inherited_snapshot_id_)
    , context(context_)
    , manifest_schema_id(manifest_schema_id_)
    , common_partition_specification(std::move(common_partition_specification_))
    , partition_key_description(std::move(partition_key_description_))
    , partition_spec_fields_count(partition_spec_fields_count_)
    , table_snapshot_schema_id(table_snapshot_schema_id_)
    , total_rows(total_rows_)
    , stop_flag(stop_flag_)
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
        /// This walk runs before `next` is ever entered, so it must honor the stop flag
        /// itself; `next` then stops on its first row and the incomplete ids are never read.
        if (stop_flag && stop_flag->load(std::memory_order_relaxed))
            return;

        const auto parsed_entry = manifest_file_deserializer->getParsedManifestFileEntry(row_index);
        if (parsed_entry->content_type != FileContentType::DATA || parsed_entry->status != ManifestEntryStatus::ADDED
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

    /// Iceberg requires one partition value per field of the spec the manifest was written with.
    /// This holds whether or not any of those fields ended up in the partition key.
    if (parsed_entry->partition_key_value.size() != partition_spec_fields_count)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Iceberg manifest partition tuple for file '{}' has {} values but the manifest's partition "
            "spec defines {} fields",
            parsed_entry->file_path_key,
            parsed_entry->partition_key_value.size(),
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
        LOG_DEBUG(
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

    if (parsed_entry->parsed_first_row_id.has_value())
        entry->first_row_id = parsed_entry->parsed_first_row_id;
    else if (!entry_first_row_ids.empty())
        entry->first_row_id = entry_first_row_ids[row_index];


    PruningReturnStatus pruning_status = PruningReturnStatus::NOT_PRUNED;
    if (filter_dag)
    {
        /// Compute per-column hyperrectangles for DATA files
        std::unordered_map<Int32, DB::Range> hyperrectangles;
        if (parsed_entry->content_type == FileContentType::DATA)
        {
            for (const auto & [column_id, bounds] : parsed_entry->value_bounds)
            {
                auto field_characteristics = schema_processor_ptr->tryGetFieldCharacteristics(resolved_schema_id, column_id);
                /// If we don't have column characteristics, bounds don't have any sense.
                /// This happens if the subfield is inside map or array, because we don't support
                /// name generation for such subfields (we support names of nested subfields in structs only).
                if (!field_characteristics)
                    continue;

                const auto & name_and_type = *field_characteristics;

                String left_str;
                String right_str;
                /// lower_bound and upper_bound may be NULL.
                if (!bounds.first.tryGet(left_str) || !bounds.second.tryGet(right_str))
                    continue;

                if (const auto type_id = name_and_type.type->getTypeId();
                    type_id == DB::TypeIndex::Tuple || type_id == DB::TypeIndex::Map || type_id == DB::TypeIndex::Array
                    || type_id == DB::TypeIndex::Variant)
                    continue;

                auto left = deserializeFieldFromBinaryRepr(left_str, name_and_type.type, true);
                auto right = deserializeFieldFromBinaryRepr(right_str, name_and_type.type, false);
                if (!left || !right)
                {
                    /// Pruning is skipped either way, but at scale 38 a bound that only loses its widened
                    /// form can still be a value the column holds, so this is not on its own a malformed
                    /// manifest and stays out of the warning log.
                    LOG_DEBUG(
                        getLogger("ManifestFileIterator"),
                        "Manifest file '{}' declares a bound that cannot be read as a usable range border "
                        "for column id {} of data file '{}'; skipping min/max pruning for this column",
                        path_to_manifest_file,
                        column_id,
                        parsed_entry->file_path_key.serialize());
                    continue;
                }

                /// At a non-zero scale the outward shift moves each decimal bound one integral unit, so it
                /// un-inverts any declared pair no more than `2 * 10^scale` apart. Only the values as
                /// declared expose that inversion, which is why they are read again here.
                std::optional<DB::Field> declared_left = left;
                std::optional<DB::Field> declared_right = right;
                if (DB::WhichDataType(DB::removeNullable(name_and_type.type)).isDecimal())
                {
                    declared_left = deserializeFieldFromBinaryRepr(
                        left_str, name_and_type.type, true, /*compensate_rounding=*/false);
                    declared_right = deserializeFieldFromBinaryRepr(
                        right_str, name_and_type.type, false, /*compensate_rounding=*/false);
                }

                /// A pair inverted as declared means the manifest's statistics are untrustworthy, so no
                /// range derived from them is safe to prune on. Dropping the column's bounds is therefore
                /// right where swapping or clamping them would prune on a value nothing vouches for.
                if (accurateLess(*declared_right, *declared_left))
                {
                    LOG_WARNING(
                        getLogger("ManifestFileIterator"),
                        "Manifest file '{}' declares a lower bound above the upper bound for column id "
                        "{} of data file '{}'; skipping min/max pruning for this column",
                        path_to_manifest_file,
                        column_id,
                        parsed_entry->file_path_key.serialize());
                    continue;
                }

                hyperrectangles.emplace(column_id, DB::Range(*left, true, *right, true));
            }

            addRowLineageHyperrectangles(hyperrectangles, *entry);
        }

        const ManifestFilesPruner * current_pruner = getOrCreatePruner(entry->resolved_schema_id);
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
        *schema_processor_ptr, table_snapshot_schema_id, schema_id, filter_dag.get(), *this, context);
    auto * raw_ptr = pruner.get();
    pruners_by_schema_id.emplace(schema_id, std::move(pruner));
    return raw_ptr;
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
        /// The data manifest decode tasks pass the stream's stopped flag here, so a cancelled
        /// query stops decoding mid-manifest. Checked between rows rather than by the caller,
        /// because a long stretch of pruned rows yields nothing the caller could check on.
        if (stop_flag && stop_flag->load(std::memory_order_relaxed))
            return nullptr;
        auto entry = processRow(row_index);
        if (entry)
            return entry;
    }
}

bool ManifestFileIterator::hasPartitionKey() const
{
    return partition_key_description.has_value();
}

const DB::KeyDescription & ManifestFileIterator::getPartitionKeyDescription() const
{
    if (!hasPartitionKey())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table has no partition key, but it was requested");
    return *(partition_key_description);
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
