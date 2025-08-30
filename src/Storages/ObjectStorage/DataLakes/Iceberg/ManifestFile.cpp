#include "config.h"

#if USE_AVRO

#include <compare>
#include <optional>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>

#include <Core/TypeId.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Poco/JSON/Parser.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTFunction.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

namespace DB::ErrorCodes
{
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace DB::Iceberg
{

String FileContentTypeToString(FileContentType type)
{
    switch (type)
    {
        case FileContentType::DATA:
            return "data";
        case FileContentType::POSITION_DELETE:
            return "position_deletes";
        case FileContentType::EQUALITY_DELETE:
            return "equality_deletes";
    }
    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported content type: {}", static_cast<int>(type));
}

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
            /// Iceberg store decimal values as unscaled value with two’s-complement big-endian binary
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
                DB::DecimalField<DB::Decimal32> result(unscaled_value, decimal_type->getScale());
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

const std::vector<ManifestFileEntry> & ManifestFileContent::getFilesWithoutDeleted(FileContentType content_type) const
{
    if (content_type == FileContentType::DATA)
        return data_files_without_deleted;
    else if (content_type == FileContentType::POSITION_DELETE)
        return position_deletes_files_without_deleted;
    else
        return equality_deletes_files;
}

using namespace DB;

ManifestFileContent::ManifestFileContent(
    const AvroForIcebergDeserializer & manifest_file_deserializer,
    const String & manifest_file_name,
    Int32 format_version_,
    const String & common_path,
    const IcebergSchemaProcessor & schema_processor,
    Int64 inherited_sequence_number,
    Int64 inherited_snapshot_id,
    const String & table_location,
    DB::ContextPtr context,
    const String & path_to_manifest_file_)
    : path_to_manifest_file(path_to_manifest_file_)
{
    for (const auto & column_name : {f_status, f_data_file})
    {
        if (!manifest_file_deserializer.hasPath(column_name))
            throw Exception(
                DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Required columns are not found in manifest file: {}", column_name);
    }

    if (format_version_ > 1 && !manifest_file_deserializer.hasPath(f_sequence_number))
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Required columns are not found in manifest file: {}", f_sequence_number);

    Poco::JSON::Parser parser;

    auto partition_spec_json_string = manifest_file_deserializer.tryGetAvroMetadataValue("partition-spec");
    if (!partition_spec_json_string.has_value())
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "No partition-spec in iceberg manifest file");

    Poco::Dynamic::Var partition_spec_json = parser.parse(*partition_spec_json_string);
    const Poco::JSON::Array::Ptr & partition_specification = partition_spec_json.extract<Poco::JSON::Array::Ptr>();

    DB::NamesAndTypesList partition_columns_description;
    std::shared_ptr<DB::ASTFunction> partition_key_ast = std::make_shared<DB::ASTFunction>();
    partition_key_ast->name = "tuple";
    partition_key_ast->arguments = std::make_shared<DB::ASTExpressionList>();
    partition_key_ast->children.push_back(partition_key_ast->arguments);

    auto schema_json_string = manifest_file_deserializer.tryGetAvroMetadataValue(f_schema);
    if (!schema_json_string.has_value())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot read Iceberg table: manifest file '{}' doesn't have field '{}' in its metadata",
            manifest_file_name,
            f_schema);
    Poco::Dynamic::Var json = parser.parse(*schema_json_string);
    const Poco::JSON::Object::Ptr & schema_object = json.extract<Poco::JSON::Object::Ptr>();
    Int32 manifest_schema_id = schema_object->getValue<int>(f_schema_id);

    for (size_t i = 0; i != partition_specification->size(); ++i)
    {
        auto partition_specification_field = partition_specification->getObject(static_cast<UInt32>(i));

        auto source_id = partition_specification_field->getValue<Int32>(f_source_id);
        /// NOTE: tricky part to support RENAME column in partition key. Instead of some name
        /// we use column internal number as it's name.
        auto numeric_column_name = DB::backQuote(DB::toString(source_id));
        std::optional<DB::NameAndTypePair> manifest_file_column_characteristics = schema_processor.tryGetFieldCharacteristics(manifest_schema_id, source_id);
        if (!manifest_file_column_characteristics.has_value())
            continue;
        auto transform_name = partition_specification_field->getValue<String>(f_partition_transform);
        auto partition_name = partition_specification_field->getValue<String>(f_partition_name);
        common_partition_specification.emplace_back(source_id, transform_name, partition_name);
        auto partition_ast = getASTFromTransform(transform_name, numeric_column_name);
        /// Unsupported partition key expression
        if (partition_ast == nullptr)
            continue;

        partition_key_ast->arguments->children.emplace_back(std::move(partition_ast));
        partition_columns_description.emplace_back(numeric_column_name, removeNullable(manifest_file_column_characteristics->type));
    }

    if (!partition_columns_description.empty())
        this->partition_key_description.emplace(DB::KeyDescription::getKeyFromAST(std::move(partition_key_ast), ColumnsDescription(partition_columns_description), context));

    for (size_t i = 0; i < manifest_file_deserializer.rows(); ++i)
    {
        FileContentType content_type = FileContentType::DATA;
        if (format_version_ > 1)
            content_type = FileContentType(manifest_file_deserializer.getValueFromRowByName(i, c_data_file_content, TypeIndex::Int32).safeGet<UInt64>());
        const auto status = ManifestEntryStatus(manifest_file_deserializer.getValueFromRowByName(i, f_status, TypeIndex::Int32).safeGet<UInt64>());


        if (status == ManifestEntryStatus::DELETED)
            continue;

        const auto snapshot_id_value = manifest_file_deserializer.getValueFromRowByName(i, f_snapshot_id);
        Int64 snapshot_id;

        if (snapshot_id_value.isNull())
        {
            if (status == ManifestEntryStatus::EXISTING)
            {
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "Cannot read Iceberg table: manifest file '{}' has entry with status 'EXISTING' without snapshot id",
                    manifest_file_name);
            }
            snapshot_id = inherited_snapshot_id;
        }
        else
        {
            snapshot_id = snapshot_id_value.safeGet<Int64>();
        }

        const auto schema_id_opt = schema_processor.tryGetSchemaIdForSnapshot(snapshot_id);
        if (!schema_id_opt.has_value())
        {
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Cannot read Iceberg table: manifest file '{}' has entry with snapshot_id '{}' for which write file schema is unknown",
                manifest_file_name,
                snapshot_id);
        }
        const auto schema_id = schema_id_opt.value();

        const auto file_path_key
            = manifest_file_deserializer.getValueFromRowByName(i, c_data_file_file_path, TypeIndex::String).safeGet<String>();
        const auto file_path = getProperFilePathFromMetadataInfo(manifest_file_deserializer.getValueFromRowByName(i, c_data_file_file_path, TypeIndex::String).safeGet<String>(), common_path, table_location);

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
        Field partition_value = manifest_file_deserializer.getValueFromRowByName(i, c_data_file_partition);
        auto tuple = partition_value.safeGet<Tuple>();

        DB::Row partition_key_value;
        for (const auto & value : tuple)
            partition_key_value.emplace_back(value);

        std::unordered_map<Int32, ColumnInfo> columns_infos;

        for (const auto & path : {c_data_file_value_counts, c_data_file_column_sizes, c_data_file_null_value_counts})
        {
            if (manifest_file_deserializer.hasPath(path))
            {
                Field values_count = manifest_file_deserializer.getValueFromRowByName(i, path);
                for (const auto & column_stats : values_count.safeGet<Array>())
                {
                    const auto & column_number_and_count = column_stats.safeGet<Tuple>();
                    Int32 number = column_number_and_count[0].safeGet<Int32>();
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

        if (content_type == FileContentType::DATA)
        {
            std::unordered_map<Int32, std::pair<Field, Field>> value_for_bounds;
            for (const auto & path : {c_data_file_lower_bounds, c_data_file_upper_bounds})
            {
                if (manifest_file_deserializer.hasPath(path))
                {
                    Field bounds = manifest_file_deserializer.getValueFromRowByName(i, path);
                    for (const auto & column_stats : bounds.safeGet<Array>())
                    {
                        const auto & column_number_and_bound = column_stats.safeGet<Tuple>();
                        Int32 number = column_number_and_bound[0].safeGet<Int32>();
                        const Field & bound_value = column_number_and_bound[1];

                        if (path == c_data_file_lower_bounds)
                            value_for_bounds[number].first = bound_value;
                        else
                            value_for_bounds[number].second = bound_value;

                        column_ids_which_have_bounds.insert(number);
                    }
                }
            }

            for (const auto & [column_id, bounds] : value_for_bounds)
            {
                auto field_characteristics = schema_processor.tryGetFieldCharacteristics(schema_id, column_id);
                /// If we don't have column characteristics, bounds don't have any sense.
                /// This happens if the subfield is inside map ot array, because we don't support
                /// name generation for such subfields (we support names of nested subfields in structs only).
                if (!field_characteristics)
                {
                    continue;
                }
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

                columns_infos[column_id].hyperrectangle.emplace(*left, true, *right, true);
            }
        }

        Int64 added_sequence_number = 0;

        String file_format
            = manifest_file_deserializer.getValueFromRowByName(i, c_data_file_file_format, TypeIndex::String).safeGet<String>();

        if (format_version_ > 1)
        {
            switch (status)
            {
                case ManifestEntryStatus::ADDED:
                    added_sequence_number = inherited_sequence_number;
                    break;
                case ManifestEntryStatus::EXISTING:
                {
                    auto value = manifest_file_deserializer.getValueFromRowByName(i, f_sequence_number);
                    if (value.isNull())
                        throw Exception(
                            DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                            "Data sequence number is null for the file added in another snapshot");
                    else
                        added_sequence_number = value.safeGet<UInt64>();
                    break;
                }
                case ManifestEntryStatus::DELETED:
                    added_sequence_number = inherited_sequence_number;
                    break;
            }
        }
        switch (content_type)
        {
            case FileContentType::DATA:
                this->data_files_without_deleted.emplace_back(
                    file_path_key,
                    file_path,
                    status,
                    added_sequence_number,
                    snapshot_id,
                    schema_id,
                    partition_key_value,
                    common_partition_specification,
                    columns_infos,
                    file_format,
                    /*reference_data_file = */ std::nullopt,
                    /*equality_ids*/ std::nullopt);
                break;
            case FileContentType::POSITION_DELETE:
            {
                /// reference_file_path can be absent in schema for some reason, though it is present in specification: https://iceberg.apache.org/spec/#manifests
                std::optional<String> reference_file_path = std::nullopt;
                if (manifest_file_deserializer.hasPath(c_data_file_referenced_data_file))
                {
                    Field reference_file_path_field = manifest_file_deserializer.getValueFromRowByName(i, c_data_file_referenced_data_file);
                    if (!reference_file_path_field.isNull())
                    {
                        reference_file_path = reference_file_path_field.safeGet<String>();
                    }
                }
                this->position_deletes_files_without_deleted.emplace_back(
                    file_path_key,
                    file_path,
                    status,
                    added_sequence_number,
                    snapshot_id,
                    schema_id,
                    partition_key_value,
                    common_partition_specification,
                    columns_infos,
                    file_format,
                    reference_file_path,
                    /*equality_ids*/ std::nullopt);
                break;
            }
            case FileContentType::EQUALITY_DELETE:
            {
                std::vector<Int32> equality_ids;
                if (manifest_file_deserializer.hasPath(c_data_file_equality_ids))
                {
                    Field equality_ids_field = manifest_file_deserializer.getValueFromRowByName(i, c_data_file_equality_ids);
                    for (const Field & id : equality_ids_field.safeGet<Array>())
                        equality_ids.push_back(id.safeGet<Int32>());
                }
                else
                    throw Exception(
                            DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                            "Couldn't find field {} in equality delete file entry", c_data_file_equality_ids);
                this->equality_deletes_files.emplace_back(
                    file_path_key,
                    file_path,
                    status,
                    added_sequence_number,
                    snapshot_id,
                    schema_id,
                    partition_key_value,
                    common_partition_specification,
                    columns_infos,
                    file_format,
                    /*reference_data_file = */ std::nullopt,
                    equality_ids);
                break;
            }
        }
    }
    sortManifestEntriesBySchemaId(data_files_without_deleted);
}

// We prefer files to be sorted by schema id, because it allows us to reuse ManifestFilePruner during partition and minmax pruning
void ManifestFileContent::sortManifestEntriesBySchemaId(std::vector<ManifestFileEntry> & files)
{
    std::vector<size_t> indices(files.size());
    std::iota(indices.begin(), indices.end(), 0);

    std::sort(
        indices.begin(),
        indices.end(),
        [&](size_t i, size_t j)
        {
            if (files[i].schema_id != files[j].schema_id)
            {
                return files[i].schema_id < files[j].schema_id;
            }
            return i < j;
        });

    std::vector<ManifestFileEntry> sorted_files;
    sorted_files.reserve(files.size());
    for (const auto & index : indices)
    {
        sorted_files.emplace_back(std::move(files[index]));
    }
    files = std::move(sorted_files);
}

bool ManifestFileContent::hasPartitionKey() const
{
    return partition_key_description.has_value();
}

const DB::KeyDescription & ManifestFileContent::getPartitionKeyDescription() const
{
    if (!hasPartitionKey())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table has no partition key, but it was requested");
    return *(partition_key_description);
}

bool ManifestFileContent::hasBoundsInfoInManifests() const
{
    return !column_ids_which_have_bounds.empty();
}

const std::set<Int32> & ManifestFileContent::getColumnsIDsWithBounds() const
{
    return column_ids_which_have_bounds;
}

size_t ManifestFileContent::getSizeInMemory() const
{
    size_t total_size = sizeof(ManifestFileContent);
    if (partition_key_description)
        total_size += sizeof(DB::KeyDescription);
    total_size += column_ids_which_have_bounds.size() * sizeof(Int32);
    total_size += data_files_without_deleted.capacity() * sizeof(ManifestFileEntry);
    total_size += position_deletes_files_without_deleted.capacity() * sizeof(ManifestFileEntry);
    return total_size;
}

std::optional<Int64> ManifestFileContent::getRowsCountInAllFilesExcludingDeleted(FileContentType content) const
{
    Int64 result = 0;

    for (const auto & file : getFilesWithoutDeleted(content))
    {
        /// Have at least one column with rows count
        bool found = false;
        for (const auto & [column, column_info] : file.columns_infos)
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


std::optional<Int64> ManifestFileContent::getBytesCountInAllDataFilesExcludingDeleted() const
{
    Int64 result = 0;
    for (const auto & file : data_files_without_deleted)
    {
        /// Have at least one column with bytes count
        bool found = false;
        for (const auto & [column, column_info] : file.columns_infos)
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

std::strong_ordering operator<=>(const PartitionSpecsEntry & lhs, const PartitionSpecsEntry & rhs)
{
    return std::tie(lhs.source_id, lhs.transform_name, lhs.partition_name)
        <=> std::tie(rhs.source_id, rhs.transform_name, rhs.partition_name);
}

template <typename A>
bool less(const std::vector<A> & lhs, const std::vector<A> & rhs)
{
    if (lhs.size() != rhs.size())
        return lhs.size() < rhs.size();
    return std::lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), [](const A & a, const A & b) { return a < b; });
}

bool operator<(const PartitionSpecification & lhs, const PartitionSpecification & rhs)
{
    return less(lhs, rhs);
}

bool operator<(const DB::Row & lhs, const DB::Row & rhs)
{
    return less(lhs, rhs);
}

std::weak_ordering operator<=>(const ManifestFileEntry & lhs, const ManifestFileEntry & rhs)
{
    return std::tie(lhs.common_partition_specification, lhs.partition_key_value, lhs.added_sequence_number)
        <=> std::tie(rhs.common_partition_specification, rhs.partition_key_value, rhs.added_sequence_number);
}
}

#endif
