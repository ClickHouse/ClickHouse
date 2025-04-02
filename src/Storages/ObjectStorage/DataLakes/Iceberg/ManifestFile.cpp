#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>

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
    extern const int UNSUPPORTED_METHOD;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
    extern const int LOGICAL_ERROR;
}

namespace Iceberg
{

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
            {
                int64_t scaler = lower_bound ? -10 : 10;
                int32_t scale = DB::getDecimalScale(*non_nullable_type);
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

constexpr const char * COLUMN_STATUS_NAME = "status";
constexpr const char * COLUMN_TUPLE_DATA_FILE_NAME = "data_file";
constexpr const char * COLUMN_SEQ_NUMBER_NAME = "sequence_number";

constexpr const char * SUBCOLUMN_FILE_PATH_NAME = "data_file.file_path";
constexpr const char * SUBCOLUMN_CONTENT_NAME = "data_file.content";
constexpr const char * SUBCOLUMN_PARTITION_NAME = "data_file.partition";

constexpr const char * SUBCOLUMN_VALUES_COUNT_NAME = "data_file.value_counts";
constexpr const char * SUBCOLUMN_COLUMN_SIZES_NAME = "data_file.column_sizes";
constexpr const char * SUBCOLUMN_NULL_VALUE_COUNTS_NAME = "data_file.null_value_counts";
constexpr const char * SUBCOLUMN_LOWER_BOUNDS_NAME = "data_file.lower_bounds";
constexpr const char * SUBCOLUMN_UPPER_BOUNDS_NAME = "data_file.upper_bounds";


const std::vector<ManifestFileEntry> & ManifestFileContent::getFiles() const
{
    return files;
}

Int32 ManifestFileContent::getSchemaId() const
{
    return schema_id;
}

using namespace DB;

ManifestFileContent::ManifestFileContent(
    const AvroForIcebergDeserializer & manifest_file_deserializer,
    Int32 format_version_,
    const String & common_path,
    Int32 schema_id_,
    const IcebergSchemaProcessor & schema_processor,
    Int64 inherited_sequence_number,
    const String & table_location,
    DB::ContextPtr context)
{
    this->schema_id = schema_id_;

    for (const auto & column_name : {COLUMN_STATUS_NAME, COLUMN_TUPLE_DATA_FILE_NAME})
    {
        if (!manifest_file_deserializer.hasPath(column_name))
            throw Exception(
                DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Required columns are not found in manifest file: {}", column_name);
    }

    if (format_version_ > 1 && !manifest_file_deserializer.hasPath(COLUMN_SEQ_NUMBER_NAME))
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Required columns are not found in manifest file: {}", COLUMN_SEQ_NUMBER_NAME);

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

    for (size_t i = 0; i != partition_specification->size(); ++i)
    {
        auto partition_specification_field = partition_specification->getObject(static_cast<UInt32>(i));

        auto source_id = partition_specification_field->getValue<Int32>("source-id");
        /// NOTE: tricky part to support RENAME column in partition key. Instead of some name
        /// we use column internal number as it's name.
        auto numeric_column_name = DB::backQuote(DB::toString(source_id));
        DB::NameAndTypePair manifest_file_column_characteristics = schema_processor.getFieldCharacteristics(schema_id, source_id);
        auto partition_ast = getASTFromTransform(partition_specification_field->getValue<String>("transform"), numeric_column_name);
        /// Unsupported partition key expression
        if (partition_ast == nullptr)
            continue;

        partition_key_ast->arguments->children.emplace_back(std::move(partition_ast));
        partition_columns_description.emplace_back(numeric_column_name, removeNullable(manifest_file_column_characteristics.type));
    }

    if (!partition_columns_description.empty())
        this->partition_key_description.emplace(DB::KeyDescription::getKeyFromAST(std::move(partition_key_ast), ColumnsDescription(partition_columns_description), context));

    for (size_t i = 0; i < manifest_file_deserializer.rows(); ++i)
    {
        FileContentType content_type = FileContentType::DATA;
        if (format_version_ > 1)
        {
            content_type = FileContentType(manifest_file_deserializer.getValueFromRowByName(i, SUBCOLUMN_CONTENT_NAME, TypeIndex::Int32).safeGet<UInt64>());
            if (content_type != FileContentType::DATA)
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD, "Cannot read Iceberg table: positional and equality deletes are not supported");
        }
        const auto status = ManifestEntryStatus(manifest_file_deserializer.getValueFromRowByName(i, COLUMN_STATUS_NAME, TypeIndex::Int32).safeGet<UInt64>());

        const auto file_path = getProperFilePathFromMetadataInfo(manifest_file_deserializer.getValueFromRowByName(i, SUBCOLUMN_FILE_PATH_NAME, TypeIndex::String).safeGet<String>(), common_path, table_location);

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
        Field partition_value = manifest_file_deserializer.getValueFromRowByName(i, SUBCOLUMN_PARTITION_NAME);
        auto tuple = partition_value.safeGet<Tuple>();

        DB::Row partition_key_value;
        for (const auto & value : tuple)
            partition_key_value.emplace_back(value);

        std::unordered_map<Int32, ColumnInfo> columns_infos;

        for (const auto & path : {SUBCOLUMN_VALUES_COUNT_NAME, SUBCOLUMN_COLUMN_SIZES_NAME, SUBCOLUMN_NULL_VALUE_COUNTS_NAME})
        {
            if (manifest_file_deserializer.hasPath(path))
            {
                Field values_count = manifest_file_deserializer.getValueFromRowByName(i, path);
                for (const auto & column_stats : values_count.safeGet<Array>())
                {
                    const auto & column_number_and_count = column_stats.safeGet<Tuple>();
                    Int32 number = column_number_and_count[0].safeGet<Int32>();
                    Int64 count = column_number_and_count[1].safeGet<Int64>();
                    if (path == SUBCOLUMN_VALUES_COUNT_NAME)
                        columns_infos[number].rows_count = count;
                    else if (path == SUBCOLUMN_COLUMN_SIZES_NAME)
                        columns_infos[number].bytes_size = count;
                    else
                        columns_infos[number].nulls_count = count;
                }
            }
        }

        std::unordered_map<Int32, std::pair<Field, Field>> value_for_bounds;
        for (const auto & path : {SUBCOLUMN_LOWER_BOUNDS_NAME, SUBCOLUMN_UPPER_BOUNDS_NAME})
        {
            if (manifest_file_deserializer.hasPath(path))
            {
                Field bounds = manifest_file_deserializer.getValueFromRowByName(i, path);
                for (const auto & column_stats : bounds.safeGet<Array>())
                {
                    const auto & column_number_and_bound = column_stats.safeGet<Tuple>();
                    Int32 number = column_number_and_bound[0].safeGet<Int32>();
                    const Field & bound_value = column_number_and_bound[1];

                    if (path == SUBCOLUMN_LOWER_BOUNDS_NAME)
                        value_for_bounds[number].first = bound_value;
                    else
                        value_for_bounds[number].second = bound_value;

                    column_ids_which_have_bounds.insert(number);
                }
            }
        }

        for (const auto & [column_id, bounds] : value_for_bounds)
        {
            DB::NameAndTypePair name_and_type = schema_processor.getFieldCharacteristics(schema_id, column_id);
            auto left = deserializeFieldFromBinaryRepr(bounds.first.safeGet<std::string>(), name_and_type.type, true);
            auto right = deserializeFieldFromBinaryRepr(bounds.second.safeGet<std::string>(), name_and_type.type, false);
            if (!left || !right)
                continue;

            columns_infos[column_id].hyperrectangle.emplace(*left, true, *right, true);
        }

        FileEntry file = FileEntry{DataFileEntry{file_path}};

        Int64 added_sequence_number = 0;
        if (format_version_ > 1)
        {
            switch (status)
            {
                case ManifestEntryStatus::ADDED:
                    added_sequence_number = inherited_sequence_number;
                    break;
                case ManifestEntryStatus::EXISTING:
                {
                    auto value = manifest_file_deserializer.getValueFromRowByName(i, COLUMN_SEQ_NUMBER_NAME);
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
        this->files.emplace_back(status, added_sequence_number, file, partition_key_value, columns_infos);
    }
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

std::optional<Int64> ManifestFileContent::getRowsCountInAllDataFilesExcludingDeleted() const
{
    Int64 result = 0;
    for (const auto & file : files)
    {
        /// Have at least one column with rows count
        bool found = false;
        for (const auto & [column, column_info] : file.columns_infos)
        {
            if (column_info.rows_count.has_value())
            {
                if (file.status != ManifestEntryStatus::DELETED)
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

std::optional<Int64> ManifestFileContent::getBytesCountInAllDataFiles() const
{
    Int64 result = 0;
    for (const auto & file : files)
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

}

#endif
