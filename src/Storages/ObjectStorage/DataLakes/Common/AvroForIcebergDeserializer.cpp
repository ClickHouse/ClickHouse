#include <algorithm>
#include <memory>
#include <sstream>
#include <unordered_set>
#include <Core/ColumnWithTypeAndName.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Common/UniqueLock.h>

#if USE_AVRO

#include <Columns/ColumnTuple.h>
#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Processors/Formats/Impl/JSONObjectEachRowRowOutputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <base/find_symbols.h>
#include <Common/CurrentThread.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

#if USE_AVRO_RUST
#include <Processors/Formats/Impl/Avro/AvroOcfReader.h>
#include <Processors/Formats/Impl/Avro/AvroRustPlan.h>
#include <Processors/Formats/Impl/Avro/AvroRustSchema.h>
#endif

#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int AVRO_EXCEPTION;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_DECOMPRESS;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_UUID;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int TOO_DEEP_RECURSION;
    extern const int TYPE_MISMATCH;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace DB::Iceberg
{

using namespace DB;

namespace
{

/// Whether `code` reports malformed or unsupported content of an Avro file, as opposed to the failure of the query or of
/// reading the file (cancellation, timeout, memory limit, network or storage errors, a logical error), which has nothing
/// to do with the file being a valid metadata file.
bool isAvroFormatError(int code)
{
    static const std::unordered_set<int> format_error_codes = {
        ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF,
        ErrorCodes::AVRO_EXCEPTION,
        ErrorCodes::BAD_ARGUMENTS,
        ErrorCodes::CANNOT_DECOMPRESS,
        ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED,
        ErrorCodes::CANNOT_PARSE_UUID,
        ErrorCodes::CANNOT_READ_ALL_DATA,
        ErrorCodes::ILLEGAL_COLUMN,
        ErrorCodes::INCORRECT_DATA,
        ErrorCodes::TOO_DEEP_RECURSION,
        ErrorCodes::TYPE_MISMATCH,
        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE};
    return format_error_codes.contains(code);
}

/// A file that is not a valid Avro file, or is one that does not have the shape of a metadata file, is reported with
/// the path of the file and the legacy error code. Any other exception, among them the violations of the metadata
/// contract, cancellation and the errors of the query or of the storage, is rethrown as it is. Has to be called from a
/// handler of `e`.
[[noreturn]] void rethrowAsMetadataError(const std::exception & e, const IcebergPathFromMetadata & manifest_file_path)
{
    const auto * db_exception = dynamic_cast<const Exception *>(&e);
    if (db_exception && !isAvroFormatError(db_exception->code()))
        throw;
    throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read Iceberg avro manifest file '{}': {}", manifest_file_path, e.what());
}

#if USE_AVRO_RUST
void checkRecordRoot(const AvroRust::OcfReader & file_reader)
{
    const auto root = file_reader.schema().root();
    if (root.kind() != AVRO_KIND_RECORD)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Avro file has root schema type {}, but Iceberg and Paimon metadata entries must be records",
            AvroRust::kindName(static_cast<UInt8>(root.kind())));
}

/// The kind of the values of a node: a union of `null` and one other type is that other type, as for the legacy
/// `avro::GenericDatum` reader, which judged a value by the type of the branch it holds. A union of several types stays a
/// union, and a union of nothing but `null` is `null`.
AvroKind valueKind(AvroRust::Node node)
{
    if (node.kind() != AVRO_KIND_UNION)
        return node.kind();
    size_t non_null_branches = 0;
    std::optional<AvroRust::Node> non_null_branch;
    for (UInt32 i = 0; i < node.childCount(); ++i)
    {
        const auto branch = node.child(i);
        if (branch.kind() != AVRO_KIND_NULL)
        {
            ++non_null_branches;
            non_null_branch = branch;
        }
    }
    if (non_null_branches == 0)
        return AVRO_KIND_NULL;
    return non_null_branches == 1 ? valueKind(*non_null_branch) : AVRO_KIND_UNION;
}

/// The kinds of the top level fields of the root record, by name.
std::unordered_map<std::string, UInt8> topLevelFieldKinds(const AvroRust::OcfReader & file_reader)
{
    const auto root = file_reader.schema().root();
    std::unordered_map<std::string, UInt8> result;
    for (UInt32 i = 0; i < root.childCount(); ++i)
        result.emplace(std::string(root.fieldName(i)), static_cast<UInt8>(valueKind(root.child(i))));
    return result;
}

/// The whole record of a file as one `Tuple` column, one element per top level field, exactly as the schema inference
/// types it. `Plan` decodes the fields of `header` (all of them here) straight into the elements.
struct RecordShape
{
    std::shared_ptr<const DataTypeTuple> data_type;
    Block header;
    Strings names;
    DataTypes types;
};

RecordShape inferRecordShape(const AvroRust::OcfReader & file_reader)
{
    checkRecordRoot(file_reader);
    auto data_type = std::dynamic_pointer_cast<const DataTypeTuple>(AvroRust::inferType(file_reader.schema().root()));
    ColumnsWithTypeAndName header_columns;
    header_columns.reserve(data_type->getElements().size());
    for (size_t i = 0; i < data_type->getElements().size(); ++i)
        header_columns.emplace_back(data_type->getElements()[i], data_type->getElementNames()[i]);
    return {data_type, Block(header_columns), data_type->getElementNames(), data_type->getElements()};
}

/// Per top level field, one flag per row that tells an Avro null from a value, allocated only once a null is met in a
/// field whose type is not `Nullable`: such a field holds the default value of its type for the null. Appends the flags
/// of a batch of `batch_rows` rows that starts at `first_row`; `missing[i][row]` is set for a value that was not read:
/// the Avro null of a union.
void appendNullFlags(
    std::vector<std::vector<UInt8>> & null_flags,
    const DataTypes & types,
    const std::vector<std::vector<UInt8>> & missing,
    size_t first_row,
    size_t batch_rows)
{
    for (size_t i = 0; i < null_flags.size(); ++i)
    {
        if (types[i]->isNullable() || std::none_of(missing[i].begin(), missing[i].end(), [](UInt8 flag) { return flag != 0; }))
            continue;
        null_flags[i].resize(first_row + batch_rows, 0);
        std::copy(missing[i].begin(), missing[i].end(), null_flags[i].begin() + first_row);
    }
}

/// The flags of the fields that have a null, by the name of the field, for `total_rows` rows.
std::unordered_map<std::string, std::vector<UInt8>>
takeNullFlags(std::vector<std::vector<UInt8>> && null_flags, const Strings & names, size_t total_rows)
{
    std::unordered_map<std::string, std::vector<UInt8>> result;
    for (size_t i = 0; i < null_flags.size(); ++i)
    {
        if (null_flags[i].empty())
            continue;
        null_flags[i].resize(total_rows, 0);
        result.emplace(names[i], std::move(null_flags[i]));
    }
    return result;
}
#endif

}

AvroForIcebergDeserializer::AvroForIcebergDeserializer(
    std::unique_ptr<ReadBufferFromFileBase> buffer_,
    const IcebergPathFromMetadata & manifest_file_path_,
    const DB::FormatSettings & format_settings)
try
    : manifest_file_path(manifest_file_path_)
{
    auto buffer = std::move(buffer_);
#if USE_AVRO_RUST
    AvroRust::OcfReader file_reader(*buffer);
    const auto record = inferRecordShape(file_reader);

    AvroRust::Plan plan(record.header, file_reader.schema(), /* allow_missing_fields = */ true, /* null_as_default = */ true, format_settings);
    MutableColumns columns = record.header.cloneEmptyColumns();
    std::vector<std::vector<UInt8>> missing;
    std::vector<std::vector<UInt8>> null_flags(columns.size());
    size_t total_rows = 0;
    while (file_reader.nextBlock())
    {
        /// Decode in bounded batches so declared block rows neither reserve all memory up front nor defer cancellation
        /// until the whole block is materialized.
        while (file_reader.rowsLeft())
        {
            const UInt64 batch_rows = std::min<UInt64>(file_reader.rowsLeft(), CANCELLATION_CHECK_PERIOD_ROWS);
            file_reader.decodeRows(plan, batch_rows, columns, missing);
            appendNullFlags(null_flags, record.types, missing, total_rows, batch_rows);
            total_rows += batch_rows;
        }
    }

    metadata = file_reader.metadata();
    bytes_read = buffer->count();
    for (auto & column : columns)
        column->finalize();
    null_rows = takeNullFlags(std::move(null_flags), record.names, total_rows);
    field_kinds = topLevelFieldKinds(file_reader);
    if (columns.empty())
        parsed_column = ColumnTuple::create(total_rows);
    else
        parsed_column = ColumnTuple::create(std::move(columns));
    parsed_column_data_type = record.data_type;
#else
    auto manifest_file_reader
        = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(*buffer), MAX_AVRO_SCHEMA_DEPTH);

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
    bytes_read = buffer->count();
    parsed_column = std::move(columns[0]);
    parsed_column_data_type = std::dynamic_pointer_cast<const DataTypeTuple>(data_type);
#endif
    parsed_manifest_file_entries.resize(parsed_column->size());
}
catch (const std::exception & e)
{
    rethrowAsMetadataError(e, manifest_file_path_);
}

#if USE_AVRO_RUST
AvroForIcebergDeserializer::AvroForIcebergDeserializer(
    const IcebergPathFromMetadata & manifest_file_path_,
    std::shared_ptr<const DataTypeTuple> data_type,
    MutableColumns columns,
    size_t rows_,
    std::unordered_map<std::string, std::vector<UInt8>> null_rows_,
    std::unordered_map<std::string, UInt8> field_kinds_,
    const std::map<std::string, std::vector<uint8_t>> & metadata_,
    size_t bytes_read_)
    : manifest_file_path(manifest_file_path_)
    , parsed_column(columns.empty() ? ColumnTuple::create(rows_) : ColumnTuple::create(std::move(columns)))
    , parsed_column_data_type(std::move(data_type))
    , metadata(metadata_)
    , bytes_read(bytes_read_)
    , null_rows(std::move(null_rows_))
    , field_kinds(std::move(field_kinds_))
{
    parsed_manifest_file_entries.resize(parsed_column->size());
}

void AvroForIcebergDeserializer::forEachBatch(
    std::unique_ptr<ReadBufferFromFileBase> buffer_,
    const IcebergPathFromMetadata & manifest_file_path,
    const DB::FormatSettings & format_settings,
    const std::function<void(const AvroForIcebergDeserializer & batch)> & callback)
{
    bool in_callback = false;
    try
    {
        auto buffer = std::move(buffer_);
        AvroRust::OcfReader file_reader(*buffer);
        const auto record = inferRecordShape(file_reader);
        const auto field_kinds = topLevelFieldKinds(file_reader);
        AvroRust::Plan plan(record.header, file_reader.schema(), /* allow_missing_fields = */ true, /* null_as_default = */ true, format_settings);
        AvroRust::forEachDecodedBatch(
            file_reader,
            plan,
            record.header,
            [&](MutableColumns & columns, const std::vector<std::vector<UInt8>> & missing, UInt64 rows)
            {
                std::vector<std::vector<UInt8>> null_flags(columns.size());
                appendNullFlags(null_flags, record.types, missing, 0, rows);
                for (auto & column : columns)
                    column->finalize();
                const AvroForIcebergDeserializer batch(
                    manifest_file_path,
                    record.data_type,
                    std::move(columns),
                    rows,
                    takeNullFlags(std::move(null_flags), record.names, rows),
                    field_kinds,
                    file_reader.metadata(),
                    buffer->count());
                in_callback = true;
                callback(batch);
                in_callback = false;
            });
    }
    catch (const std::exception & e)
    {
        if (in_callback)
            throw;
        rethrowAsMetadataError(e, manifest_file_path);
    }
}
#endif

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
    return WhichDataType(getDataTypeForPath(path)).idx;
}

DataTypePtr AvroForIcebergDeserializer::getDataTypeForPath(const std::string & path) const
{
    auto & subcolumn_with_type = extractSubcolumnWithType(path);
    if (!subcolumn_with_type.has_value())
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Cannot find column {} in manifest file {}", path, manifest_file_path);
    return subcolumn_with_type->second;
}

Int64 AvroForIcebergDeserializer::getFormatVersionFromManifestFileMetadata() const
{
    auto format_version_value = tryGetAvroMetadataValue("format-version");
    if (format_version_value.has_value())
    {
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

    /// Older ClickHouse versions wrote both manifest lists and manifest files without the
    /// `format-version` Avro metadata key, so we fall back to schema-based detection: the
    /// `sequence_number` field appears at the top level of v2 manifest lists and v2
    /// manifest entries, but is absent from their v1 counterparts.
    if (hasPath(f_sequence_number))
        return 2;
    return 1;
}


ParsedManifestFileEntryPtr AvroForIcebergDeserializer::createParsedManifestFileEntry(size_t row_index) const
{
    const auto format_version = getFormatVersionFromManifestFileMetadata();
    FileContentType content_type = FileContentType::DATA;
    if (format_version > 1 && hasPath(c_data_file_content))
    {
        /// The value comes from the file and has to be validated: casting an arbitrary integer to
        /// the enum and switching over it below would be undefined behaviour.
        const auto content_type_value = getValueFromRowByName(row_index, c_data_file_content, TypeIndex::Int32).safeGet<UInt64>();
        if (content_type_value > UInt64(FileContentType::EQUALITY_DELETE))
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Cannot read Iceberg table: unexpected value {} of 'data_file.content' in a manifest file",
                content_type_value);
        content_type = FileContentType(content_type_value);
    }

    const auto status_value = getValueFromRowByName(row_index, f_status, TypeIndex::Int32).safeGet<UInt64>();
    if (status_value > UInt64(ManifestEntryStatus::DELETED))
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Cannot read Iceberg table: unexpected value {} of 'status' in a manifest file",
            status_value);
    const auto status = ManifestEntryStatus(status_value);

    /// Iceberg v2 makes `snapshot_id` optional on the manifest entry: a missing Avro column
    /// or a null value is inherited from the manifest list (`added_snapshot_id`). Not gated
    /// on `format_version > 1` because the version comes from the Avro `format-version` key
    /// or, failing that, from the presence of `sequence_number` -- a writer omitting both
    /// would be misclassified as v1 and rejected here.
    std::optional<Int64> snapshot_id;
    const bool has_snapshot_id_column = hasPath(f_snapshot_id);
    if (has_snapshot_id_column)
    {
        const auto snapshot_id_value = getValueFromRowByName(row_index, f_snapshot_id);
        if (!snapshot_id_value.isNull())
            snapshot_id = snapshot_id_value.safeGet<Int64>();
    }
    /// EXISTING entries must carry an explicit snapshot id: Compaction.cpp restores their
    /// lineage from `parsed_snapshot_id` and would otherwise substitute this manifest's
    /// `added_snapshot_id`, which is not the snapshot that originally added the file.
    if (!snapshot_id.has_value() && status == ManifestEntryStatus::EXISTING)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Cannot read Iceberg table: manifest file '{}' has an EXISTING entry without a snapshot id "
            "(the snapshot_id column is {})",
            manifest_file_path,
            has_snapshot_id_column ? "null" : "absent");

    /// `sequence_number` is optional on a v2 manifest entry for the same reason as `snapshot_id`, so a
    /// missing column is left unresolved rather than substituted with 0. A literal 0 reads as an assigned
    /// value, which skips inheritance in ManifestFileIterator and makes an ADDED data file look older than
    /// it is; a manifest rewrite would then persist that 0 as the entry's explicit data sequence number.
    std::optional<Int64> sequence_number;
    if (format_version > 1)
    {
        const bool has_sequence_number_column = hasPath(f_sequence_number);
        if (has_sequence_number_column)
        {
            const auto sequence_number_value = getValueFromRowByName(row_index, f_sequence_number);
            if (!sequence_number_value.isNull())
                sequence_number = sequence_number_value.safeGet<Int64>();
        }
        /// The spec inherits data and file sequence numbers only for ADDED entries; EXISTING and DELETED
        /// must carry them explicitly (https://iceberg.apache.org/spec/#sequence-number-inheritance).
        if (!sequence_number.has_value() && status == ManifestEntryStatus::EXISTING)
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Cannot read Iceberg table: manifest file '{}' has an EXISTING entry without a data "
                "sequence number (the sequence_number column is {})",
                manifest_file_path,
                has_sequence_number_column ? "null" : "absent");
    }

    /// `file_sequence_number` can differ from the data `sequence_number` and, like it, is inherited from the
    /// manifest's sequence number when null. Keep it raw here; the inherited value is resolved by the caller.
    std::optional<Int64> file_sequence_number;

    if (format_version > 1 && hasPath(f_file_sequence_number))
    {
        const auto file_sequence_number_value = getValueFromRowByName(row_index, f_file_sequence_number);
        if (!file_sequence_number_value.isNull())
            file_sequence_number = file_sequence_number_value.safeGet<Int64>();
    }

    std::optional<UInt64> first_row_id;

    if (format_version > 2 && hasPath(c_data_file_first_row_id))
    {
        const auto first_row_id_value = getValueFromRowByName(row_index, c_data_file_first_row_id);
        if (!first_row_id_value.isNull())
        {
            first_row_id = first_row_id_value.safeGet<Int64>();
        }
    }

    const auto file_path_key = IcebergPathFromMetadata::deserialize(
        getValueFromRowByName(row_index, c_data_file_file_path, TypeIndex::String).safeGet<String>());
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

    const auto record_count = getValueFromRowByName(row_index, c_data_file_record_count, TypeIndex::Int64).safeGet<Int64>();
    const auto file_size_in_bytes = getValueFromRowByName(row_index, c_data_file_file_size_in_bytes, TypeIndex::Int64).safeGet<Int64>();

    std::optional<Iceberg::IcebergPathFromMetadata> referenced_data_file_path;
    if (hasPath(c_data_file_referenced_data_file))
    {
        Field referenced_data_file_field = getValueFromRowByName(row_index, c_data_file_referenced_data_file);
        if (!referenced_data_file_field.isNull())
            referenced_data_file_path.emplace(Iceberg::IcebergPathFromMetadata::deserialize(referenced_data_file_field.safeGet<String>()));
    }

    std::optional<Int64> content_offset;
    if (hasPath(c_data_file_content_offset))
    {
        Field content_offset_field = getValueFromRowByName(row_index, c_data_file_content_offset);
        if (!content_offset_field.isNull())
            content_offset = content_offset_field.safeGet<Int64>();
    }

    std::optional<Int64> content_size_in_bytes;
    if (hasPath(c_data_file_content_size_in_bytes))
    {
        Field content_size_field = getValueFromRowByName(row_index, c_data_file_content_size_in_bytes);
        if (!content_size_field.isNull())
            content_size_in_bytes = content_size_field.safeGet<Int64>();
    }

    std::optional<std::vector<Int64>> split_offsets;
    if (hasPath(c_data_file_split_offsets))
    {
        Field split_offsets_field = getValueFromRowByName(row_index, c_data_file_split_offsets);
        if (!split_offsets_field.isNull())
        {
            split_offsets.emplace();
            for (const Field & offset : split_offsets_field.safeGet<Array>())
                split_offsets->push_back(offset.safeGet<Int64>());
        }
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
                file_sequence_number,
                snapshot_id,
                first_row_id,
                partition_key_value,
                columns_infos,
                value_for_bounds,
                file_format,
                /*lower_reference_data_file_path_ = */ std::nullopt,
                /*upper_reference_data_file_path_ = */ std::nullopt,
                /*referenced_data_file_path_ = */ std::nullopt,
                /*content_offset_ = */ std::nullopt,
                /*content_size_in_bytes_ = */ std::nullopt,
                /*equality_ids*/ std::nullopt,
                sort_order_id,
                record_count,
                file_size_in_bytes,
                split_offsets);
        }
        case FileContentType::POSITION_DELETE: {
            /// reference_file_path can be absent in schema for some reason, though it is present in specification: https://iceberg.apache.org/spec/#manifests
            std::optional<Iceberg::IcebergPathFromMetadata> lower_reference_data_file_path;
            std::optional<Iceberg::IcebergPathFromMetadata> upper_reference_data_file_path;
            bool bounds_set_by_referenced_data_file = false;
            if (referenced_data_file_path.has_value())
            {
                lower_reference_data_file_path = referenced_data_file_path;
                upper_reference_data_file_path = referenced_data_file_path;
                bounds_set_by_referenced_data_file = true;
            }
            if (!bounds_set_by_referenced_data_file)
            {
                if (auto it = value_for_bounds.find(IcebergPositionDeleteTransform::data_file_path_column_field_id);
                    it != value_for_bounds.end())
                {
                    auto & [lower, upper] = it->second;
                    if (!lower.isNull())
                        lower_reference_data_file_path.emplace(Iceberg::IcebergPathFromMetadata::deserialize(lower.safeGet<String>()));
                    if (!upper.isNull())
                        upper_reference_data_file_path.emplace(Iceberg::IcebergPathFromMetadata::deserialize(upper.safeGet<String>()));

                    /// A lower bound sorting above the upper bound describes no range of paths, so it cannot say
                    /// which data files this delete file references. Dropping both leaves them as a manifest that
                    /// declares no bounds does.
                    if (lower_reference_data_file_path.has_value() && upper_reference_data_file_path.has_value()
                        && *upper_reference_data_file_path < *lower_reference_data_file_path)
                    {
                        LOG_WARNING(
                            getLogger("AvroForIcebergDeserializer"),
                            "Manifest file '{}' declares a reference data file lower bound above its upper bound for "
                            "position delete file '{}' ('{}' > '{}'); ignoring both bounds",
                            manifest_file_path.serialize(),
                            file_path_key.serialize(),
                            lower_reference_data_file_path->serialize(),
                            upper_reference_data_file_path->serialize());
                        lower_reference_data_file_path.reset();
                        upper_reference_data_file_path.reset();
                    }
                }
            }
            return std::make_shared<const ParsedManifestFileEntry>(
                FileContentType::POSITION_DELETE,
                file_path_key,
                row_index,
                status,
                sequence_number,
                file_sequence_number,
                snapshot_id,
                first_row_id,
                partition_key_value,
                columns_infos,
                value_for_bounds,
                file_format,
                lower_reference_data_file_path,
                upper_reference_data_file_path,
                referenced_data_file_path,
                content_offset,
                content_size_in_bytes,
                /*equality_ids*/ std::nullopt,
                /*sort_order_id = */ std::nullopt,
                record_count,
                file_size_in_bytes);
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
            if (equality_ids.empty())
                throw Exception(
                    DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "Field {} is empty in equality delete file entry, but at least one equality field id is required",
                    c_data_file_equality_ids);
            return std::make_shared<const ParsedManifestFileEntry>(
                FileContentType::EQUALITY_DELETE,
                file_path_key,
                row_index,
                status,
                sequence_number,
                file_sequence_number,
                snapshot_id,
                first_row_id,
                partition_key_value,
                columns_infos,
                value_for_bounds,
                file_format,
                /*lower_reference_data_file_path_ = */ std::nullopt,
                /*upper_reference_data_file_path_ = */ std::nullopt,
                /*referenced_data_file_path_ = */ std::nullopt,
                /*content_offset_ = */ std::nullopt,
                /*content_size_in_bytes_ = */ std::nullopt,
                equality_ids,
                /*sort_order_id = */ std::nullopt,
                record_count,
                file_size_in_bytes);
        }
    }

    throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
        "Cannot read Iceberg table: unexpected content type {} of a manifest file entry", UInt64(content_type));
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

#if USE_AVRO_RUST
bool AvroForIcebergDeserializer::isNullInRow(size_t row_num, const std::string & field) const
{
    if (row_num >= rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Row {} is out of range of manifest file {} with {} rows", row_num, manifest_file_path, rows());

    const auto it = null_rows.find(field);
    return it != null_rows.end() && it->second[row_num] != 0;
}

std::optional<UInt8> AvroForIcebergDeserializer::getAvroKindOfField(const std::string & field) const
{
    const auto it = field_kinds.find(field);
    if (it == field_kinds.end())
        return std::nullopt;
    return it->second;
}
#endif

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
