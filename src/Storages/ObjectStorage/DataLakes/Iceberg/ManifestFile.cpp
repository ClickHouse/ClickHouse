#include "config.h"

#if USE_AVRO

#include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileImpl.h"
#include "Storages/ObjectStorage/DataLakes/Iceberg/Utils.h"

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Poco/JSON/Parser.h>
#include "DataTypes/DataTypeTuple.h"
#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTFunction.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB::ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int UNSUPPORTED_METHOD;
extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace Iceberg
{

constexpr const char * COLUMN_STATUS_NAME = "status";
constexpr const char * COLUMN_TUPLE_DATA_FILE_NAME = "data_file";
constexpr const char * COLUMN_SEQ_NUMBER_NAME = "sequence_number";

constexpr const char * SUBCOLUMN_FILE_PATH_NAME = "file_path";
constexpr const char * SUBCOLUMN_CONTENT_NAME = "content";
constexpr const char * SUBCOLUMN_PARTITION_NAME = "partition";


const std::vector<ManifestFileEntry> & ManifestFileContent::getFiles() const
{
    return impl->files;
}

Int32 ManifestFileContent::getSchemaId() const
{
    return impl->schema_id;
}


ManifestFileContent::ManifestFileContent(std::unique_ptr<ManifestFileContentImpl> impl_) : impl(std::move(impl_))
{
}

using namespace DB;


ManifestFileContentImpl::ManifestFileContentImpl(
    std::unique_ptr<avro::DataFileReaderBase> manifest_file_reader_,
    Int32 format_version_,
    const String & common_path,
    const DB::FormatSettings & format_settings,
    Int32 schema_id_,
    const IcebergSchemaProcessor & schema_processor,
    Int64 inherited_sequence_number,
    const String & table_location,
    DB::ContextPtr context)
{
    this->schema_id = schema_id_;

    avro::NodePtr root_node = manifest_file_reader_->dataSchema().root();

    auto [name_to_index, name_to_data_type, manifest_file_header] = getColumnsAndTypesFromAvroByNames(
        root_node,
        {COLUMN_STATUS_NAME, COLUMN_TUPLE_DATA_FILE_NAME, COLUMN_SEQ_NUMBER_NAME},
        {avro::Type::AVRO_INT, avro::Type::AVRO_RECORD, avro::Type::AVRO_UNION});

    for (const auto & column_name : {COLUMN_STATUS_NAME, COLUMN_TUPLE_DATA_FILE_NAME})
    {
        if (name_to_index.find(column_name) == name_to_index.end())
            throw Exception(
                DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Required columns are not found in manifest file: {}", column_name);
    }

    if (format_version_ > 1 && name_to_index.find(COLUMN_SEQ_NUMBER_NAME) == name_to_index.end())
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Required columns are not found in manifest file: {}", COLUMN_SEQ_NUMBER_NAME);

    auto columns = parseAvro(*manifest_file_reader_, manifest_file_header, format_settings);
    if (columns.at(name_to_index.at(COLUMN_STATUS_NAME))->getDataType() != TypeIndex::Int32)
    {
        throw Exception(
            DB::ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `{}` field should be Int32 type, got {}",
            COLUMN_STATUS_NAME,
            columns.at(name_to_index.at(COLUMN_STATUS_NAME))->getFamilyName());
    }
    if (columns.at(name_to_index.at(COLUMN_TUPLE_DATA_FILE_NAME))->getDataType() != TypeIndex::Tuple)
    {
        throw Exception(
            DB::ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `{}` field should be Tuple type, got {}",
            COLUMN_TUPLE_DATA_FILE_NAME,
            magic_enum::enum_name(columns.at(name_to_index.at(COLUMN_TUPLE_DATA_FILE_NAME))->getDataType()));
    }

    const auto * status_int_column = assert_cast<DB::ColumnInt32 *>(columns.at(name_to_index.at(COLUMN_STATUS_NAME)).get());

    const auto & data_file_tuple_type = assert_cast<const DataTypeTuple &>(*name_to_data_type.at(COLUMN_TUPLE_DATA_FILE_NAME).get());
    const auto * data_file_tuple_column = assert_cast<DB::ColumnTuple *>(columns.at(name_to_index.at(COLUMN_TUPLE_DATA_FILE_NAME)).get());

    ColumnPtr file_path_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName(SUBCOLUMN_FILE_PATH_NAME));

    if (file_path_column->getDataType() != TypeIndex::String)
    {
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `{}` field should be String type, got {}",
            SUBCOLUMN_FILE_PATH_NAME,
            magic_enum::enum_name(file_path_column->getDataType()));
    }

    const auto * file_path_string_column = assert_cast<const ColumnString *>(file_path_column.get());

    ColumnPtr content_column;
    const ColumnInt32 * content_int_column = nullptr;
    if (format_version_ > 1)
    {
        content_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName(SUBCOLUMN_CONTENT_NAME));
        if (content_column->getDataType() != TypeIndex::Int32)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `{}` field should be Int type, got {}",
                SUBCOLUMN_CONTENT_NAME,
                magic_enum::enum_name(content_column->getDataType()));
        }

        content_int_column = assert_cast<const ColumnInt32 *>(content_column.get());
    }


    Poco::JSON::Parser parser;

    ColumnPtr big_partition_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName(SUBCOLUMN_PARTITION_NAME));
    if (big_partition_column->getDataType() != TypeIndex::Tuple)
    {
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `{}` field should be Tuple type, got {}",
            SUBCOLUMN_PARTITION_NAME,
            magic_enum::enum_name(big_partition_column->getDataType()));
    }
    const auto * big_partition_tuple = assert_cast<const ColumnTuple *>(big_partition_column.get());

    auto avro_metadata = manifest_file_reader_->metadata();

    std::vector<uint8_t> partition_spec_json_bytes = avro_metadata["partition-spec"];
    String partition_spec_json_string
        = String(reinterpret_cast<char *>(partition_spec_json_bytes.data()), partition_spec_json_bytes.size());

    Poco::Dynamic::Var partition_spec_json = parser.parse(partition_spec_json_string);
    const Poco::JSON::Array::Ptr & partition_specification = partition_spec_json.extract<Poco::JSON::Array::Ptr>();

    std::vector<ColumnPtr> partition_columns;
    DB::NamesAndTypesList partition_columns_description;
    std::shared_ptr<DB::ASTFunction> partition_key_ast = std::make_shared<DB::ASTFunction>();
    partition_key_ast->name = "tuple";
    partition_key_ast->arguments = std::make_shared<DB::ASTExpressionList>();
    partition_key_ast->children.push_back(partition_key_ast->arguments);

    for (size_t i = 0; i != partition_specification->size(); ++i)
    {
        auto current_field = partition_specification->getObject(static_cast<UInt32>(i));

        auto source_id = current_field->getValue<Int32>("source-id");
        DB::NameAndTypePair current_column = schema_processor.getFieldCharacteristics(schema_id, source_id);

        auto partition_ast = getASTFromTransform(current_field->getValue<String>("transform"), current_column.name);
        /// Unsupported partition key expression
        if (partition_ast == nullptr)
            continue;

        partition_key_ast->arguments->children.push_back(partition_ast);
        partition_columns_description.emplace_back(current_column.name, removeNullable(current_column.type));
        partition_columns.push_back(removeNullable(big_partition_tuple->getColumnPtr(i)));
        this->partition_column_ids.push_back(source_id);
    }

    this->partition_key_description = DB::KeyDescription::getKeyFromAST(partition_key_ast, ColumnsDescription(partition_columns_description), context);

    std::optional<const ColumnNullable *> sequence_number_column = std::nullopt;
    if (format_version_ > 1)
    {
        if (columns.at(name_to_index.at(COLUMN_SEQ_NUMBER_NAME))->getDataType() != TypeIndex::Nullable)
        {
            throw Exception(
                DB::ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `{}` field should be Nullable type, got {}",
                COLUMN_SEQ_NUMBER_NAME,
                magic_enum::enum_name(columns.at(name_to_index.at(COLUMN_SEQ_NUMBER_NAME))->getDataType()));
        }
        sequence_number_column = assert_cast<const ColumnNullable *>(columns.at(name_to_index.at(COLUMN_SEQ_NUMBER_NAME)).get());
        if (sequence_number_column.value()->getNestedColumnPtr()->getDataType() != TypeIndex::Int64)
        {
            throw Exception(
                DB::ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `{}` field should be Int64 type, got {}",
                COLUMN_SEQ_NUMBER_NAME,
                magic_enum::enum_name(sequence_number_column.value()->getNestedColumnPtr()->getDataType()));
        }
    }

    for (size_t i = 0; i < data_file_tuple_column->size(); ++i)
    {
        FileContentType content_type = FileContentType::DATA;
        if (format_version_ > 1)
        {
            content_type = FileContentType(content_int_column->getElement(i));
            if (content_type != FileContentType::DATA)
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD, "Cannot read Iceberg table: positional and equality deletes are not supported");
        }
        const auto status = ManifestEntryStatus(status_int_column->getInt(i));

        const auto file_path = getProperFilePathFromMetadataInfo(file_path_string_column->getDataAt(i).toView(), common_path, table_location);

        DB::Row partition_key_value;
        std::unordered_map<Int32, Range> partition_ranges;
        for (size_t j = 0; j < partition_columns.size(); ++j)
        {
            Field partition_value;
            partition_columns[j]->get(i, partition_value);
            partition_key_value.emplace_back(partition_value);
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
                    if (sequence_number_column.value()->isNullAt(i))
                        throw Exception(
                            DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                            "Data sequence number is null for the file added in another snapshot");
                    else
                        added_sequence_number = sequence_number_column.value()->getInt(i);
                    break;
                case ManifestEntryStatus::DELETED:
                    added_sequence_number = inherited_sequence_number;
                    break;
            }
        }
        this->files.emplace_back(status, added_sequence_number, file, partition_key_value);
    }
}

DB::KeyDescription ManifestFileContent::getPartitionKeyDescription() const
{
    return impl->partition_key_description;
}

std::vector<Int32> ManifestFileContent::getPartitionKeyColumnIDs() const
{
    return impl->partition_column_ids;
}

}

#endif
