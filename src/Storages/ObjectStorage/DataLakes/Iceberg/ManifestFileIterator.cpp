#include <base/scope_guard.h>
#include "config.h"

#if USE_AVRO

#include <compare>
#include <optional>

#include <Interpreters/IcebergMetadataLog.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <Core/TypeId.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Poco/JSON/Parser.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTFunction.h>
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
    /// Iceberg stores lower_bounds and upper_bounds serialized with some custom deserialization as bytes array
    /// https://iceberg.apache.org/spec/#appendix-d-single-value-serialization
    std::optional<DB::Field> deserializeFieldFromBinaryRepr(std::string str, DB::DataTypePtr expected_type, bool lower_bound)
    {
        auto non_nullable_type = DB::removeNullable(expected_type);
        auto column = non_nullable_type->createColumn();
        if (DB::WhichDataType(non_nullable_type).isDecimal())
        {
            /// Iceberg store decimal values as unscaled value with two's-complement big-endian binary
            /// using the minimum number of bytes for the value
            /// Our decimal binary representation is little endian
            /// so we cannot reuse our default code for parsing it.
            int64_t unscaled_value = 0;

            // Convert from big-endian to signed int
            for (const auto byte : str)
                unscaled_value = (unscaled_value << 8) | static_cast<uint8_t>(byte);

            /// Add sign
            if (str[0] & 0x80)
            {
                int64_t sign_extension = -1;
                sign_extension <<= (str.size() * 8);
                unscaled_value |= sign_extension;
            }

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
            if (int32_t scale = DB::getDecimalScale(*non_nullable_type))
            {
                int64_t scaler = lower_bound ? -10 : 10;
                while (--scale)
                    scaler *= 10;

                unscaled_value += scaler;
            }

            if (const auto * decimal_type = DB::checkDecimal<DB::Decimal32>(*non_nullable_type))
            {
                DB::DecimalField<DB::Decimal32> result(static_cast<Int32>(unscaled_value), decimal_type->getScale());
                return result;
            }
            if (const auto * decimal_type = DB::checkDecimal<DB::Decimal64>(*non_nullable_type))
            {
                DB::DecimalField<DB::Decimal64> result(unscaled_value, decimal_type->getScale());
                return result;
            }
            else
            {
                return std::nullopt;
            }
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

std::optional<Int64> ManifestFileIterator::ManifestFileEntriesHandle::getRowsCountInAllFilesExcludingDeleted(FileContentType content) const
{
    Int64 result = 0;
    for (const auto & file : getFilesWithoutDeleted(content))
    {
        /// Have at least one column with rows count
        bool found = false;
        for (const auto & [column, column_info] : file->parsed_entry->columns_infos)
        {
            if (column_info.rows_count.has_value())
            {
                result += *column_info.rows_count;
                found = true;
                break;
            }
        }
        if (!found)
            return std::nullopt;
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
    const String & manifest_file_name_,
    Int32 format_version_,
    const String & common_path_,
    IcebergSchemaProcessor & schema_processor,
    Int64 inherited_sequence_number_,
    Int64 inherited_snapshot_id_,
    const String & table_location_,
    DB::ContextPtr context_,
    const String & path_to_manifest_file_,
    std::shared_ptr<const ActionsDAG> filter_dag_,
    Int32 table_snapshot_schema_id_)
{
    insertRowToLogTable(
        context_,
        manifest_file_deserializer_->getMetadataContent(),
        DB::IcebergMetadataLogLevel::ManifestFileMetadata,
        common_path_,
        path_to_manifest_file_,
        std::nullopt,
        std::nullopt);

    for (const auto & column_name : {f_status, f_data_file})
    {
        if (!manifest_file_deserializer_->hasPath(column_name))
            throw Exception(
                DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Required columns are not found in manifest file: {}", column_name);
    }

    if (format_version_ > 1 && !manifest_file_deserializer_->hasPath(f_sequence_number))
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Required columns are not found in manifest file: {}", f_sequence_number);

    Poco::JSON::Parser parser;

    auto partition_spec_json_string = manifest_file_deserializer_->tryGetAvroMetadataValue("partition-spec");
    if (!partition_spec_json_string.has_value())
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "No partition-spec in iceberg manifest file");

    Poco::Dynamic::Var partition_spec_json = parser.parse(*partition_spec_json_string);
    const Poco::JSON::Array::Ptr & partition_specification = partition_spec_json.extract<Poco::JSON::Array::Ptr>();

    DB::NamesAndTypesList partition_columns_description;
    auto partition_key_ast = make_intrusive<ASTFunction>();
    partition_key_ast->name = "tuple";
    partition_key_ast->arguments = make_intrusive<DB::ASTExpressionList>();
    partition_key_ast->children.push_back(partition_key_ast->arguments);

    auto schema_json_string = manifest_file_deserializer_->tryGetAvroMetadataValue(f_schema);
    if (!schema_json_string.has_value())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot read Iceberg table: manifest file '{}' doesn't have field '{}' in its metadata",
            manifest_file_name_,
            f_schema);

    Poco::Dynamic::Var json = parser.parse(*schema_json_string);
    const Poco::JSON::Object::Ptr & schema_object = json.extract<Poco::JSON::Object::Ptr>();
    Int32 manifest_schema_id = schema_object->getValue<int>(f_schema_id);

    schema_processor.addIcebergTableSchema(schema_object);

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
        partition_spec_vec.emplace_back(source_id, transform_name, partition_name);
        auto partition_ast = getASTFromTransform(transform_name, numeric_column_name);
        /// Unsupported partition key expression
        if (partition_ast == nullptr)
            continue;

        partition_key_ast->as<ASTFunction>()->arguments->children.emplace_back(std::move(partition_ast));
        partition_columns_description.emplace_back(numeric_column_name, removeNullable(manifest_file_column_characteristics->type));
    }

    std::optional<DB::KeyDescription> partition_key_description;
    if (!partition_columns_description.empty())
        partition_key_description.emplace(
            DB::KeyDescription::getKeyFromAST(std::move(partition_key_ast), ColumnsDescription(partition_columns_description), context_));

    size_t total_rows = manifest_file_deserializer_->rows();

    return std::shared_ptr<ManifestFileIterator>(new ManifestFileIterator(
        std::move(manifest_file_deserializer_),
        path_to_manifest_file_,
        manifest_file_name_,
        format_version_,
        common_path_,
        table_location_,
        schema_processor,
        inherited_sequence_number_,
        inherited_snapshot_id_,
        context_,
        manifest_schema_id,
        std::make_shared<const PartitionSpecification>(std::move(partition_spec_vec)),
        std::move(partition_key_description),
        total_rows,
        std::move(filter_dag_),
        table_snapshot_schema_id_));
}

ManifestFileIterator::ManifestFileIterator(
    std::shared_ptr<AvroForIcebergDeserializer> manifest_file_deserializer_,
    const String & path_to_manifest_file_,
    const String & manifest_file_name_,
    Int32 format_version_,
    const String & common_path_,
    const String & table_location_,
    IcebergSchemaProcessor & schema_processor,
    Int64 inherited_sequence_number_,
    Int64 inherited_snapshot_id_,
    DB::ContextPtr context_,
    Int32 manifest_schema_id_,
    std::shared_ptr<const PartitionSpecification> common_partition_specification_,
    std::optional<DB::KeyDescription> partition_key_description_,
    size_t total_rows_,
    std::shared_ptr<const ActionsDAG> filter_dag_,
    Int32 table_snapshot_schema_id_)
    : manifest_file_deserializer(std::move(manifest_file_deserializer_))
    , path_to_manifest_file(path_to_manifest_file_)
    , manifest_file_name(manifest_file_name_)
    , format_version(format_version_)
    , common_path(common_path_)
    , table_location(table_location_)
    , inherited_sequence_number(inherited_sequence_number_)
    , inherited_snapshot_id(inherited_snapshot_id_)
    , context(context_)
    , manifest_schema_id(manifest_schema_id_)
    , common_partition_specification(std::move(common_partition_specification_))
    , partition_key_description(std::move(partition_key_description_))
    , table_snapshot_schema_id(table_snapshot_schema_id_)
    , total_rows(total_rows_)
    , data_files_without_deleted(std::make_shared<std::vector<ProcessedManifestFileEntryPtr>>())
    , position_deletes_files_without_deleted(std::make_shared<std::vector<ProcessedManifestFileEntryPtr>>())
    , equality_deletes_files_without_deleted(std::make_shared<std::vector<ProcessedManifestFileEntryPtr>>())
    , filter_dag(std::move(filter_dag_))
    , schema_processor_ptr(&schema_processor)
{
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
            manifest_file_deserializer->getContent(row_index),
            DB::IcebergMetadataLogLevel::ManifestFileEntry,
            common_path,
            path_to_manifest_file,
            row_index,
            std::nullopt);
        return nullptr;
    }

    /// Compute inherited/resolved fields
    const auto file_path = getProperFilePathFromMetadataInfo(parsed_entry->file_path_key, common_path, table_location);

    Int64 resolved_snapshot_id;
    if (parsed_entry->parsed_snapshot_id.has_value())
    {
        resolved_snapshot_id = *parsed_entry->parsed_snapshot_id;
    }
    else if (parsed_entry->status == ManifestEntryStatus::EXISTING)
    {
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Cannot read Iceberg table: manifest file '{}' has entry with snapshot_id 'null' for which write file schema is unknown",
            manifest_file_name);
    }
    else
    {
        resolved_snapshot_id = inherited_snapshot_id;
    }

    const auto schema_id_opt = schema_processor_ptr->tryGetSchemaIdForSnapshot(resolved_snapshot_id);
    if (!schema_id_opt.has_value())
    {
        /// Error logged but not thrown to avoid breaking whole query because of backward compatibility reasons.
        /// That's actually an error because it can lead to incorrect query results, so we are creating an exception to put it to system.error_log.
        try
        {
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Cannot read Iceberg table: manifest file '{}' has entry with snapshot_id '{}' for which write file schema is unknown",
                manifest_file_name,
                resolved_snapshot_id);
        }
        catch (const Exception &)
        {
            tryLogCurrentException("ICEBERG_SPECIFICATION_VIOLATION", "", LogsLevel::error);
        }
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
        parsed_entry, common_partition_specification, file_path, resolved_sequence_number, resolved_schema_id);


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
                    type_id == DB::TypeIndex::Tuple || type_id == DB::TypeIndex::Map || type_id == DB::TypeIndex::Array)
                    continue;

                auto left = deserializeFieldFromBinaryRepr(left_str, name_and_type.type, true);
                auto right = deserializeFieldFromBinaryRepr(right_str, name_and_type.type, false);
                if (!left || !right)
                    continue;

                hyperrectangles.emplace(column_id, DB::Range(*left, true, *right, true));
            }
        }

        const ManifestFilesPruner * current_pruner = getOrCreatePruner(entry->resolved_schema_id);
        pruning_status = current_pruner->canBePruned(entry, hyperrectangles);
    }
    insertRowToLogTable(
        context,
        manifest_file_deserializer->getContent(row_index),
        DB::IcebergMetadataLogLevel::ManifestFileEntry,
        common_path,
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
