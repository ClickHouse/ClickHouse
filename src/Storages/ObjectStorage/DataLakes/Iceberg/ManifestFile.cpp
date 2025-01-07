#include <unordered_set>
#include "config.h"

#if USE_AVRO


#include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileImpl.h"
#include "Storages/ObjectStorage/DataLakes/Iceberg/Utils.h"


#include <Columns/ColumnTuple.h>
#include "DataTypes/DataTypeTuple.h"


namespace DB::ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED_METHOD;
}

namespace Iceberg
{

const std::vector<DataFileEntry> & ManifestFileContent::getDataFiles() const
{
    return impl->data_files;
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
    Int32 schema_id_)
{
    this->schema_id = schema_id_;
    avro::NodePtr root_node = manifest_file_reader_->dataSchema().root();
    size_t leaves_num = root_node->leaves();
    size_t expected_min_num = format_version_ == 1 ? 3 : 2;
    if (leaves_num < expected_min_num)
    {
        throw Exception(
            DB::ErrorCodes::BAD_ARGUMENTS, "Unexpected number of columns {}. Expected at least {}", root_node->leaves(), expected_min_num);
    }

    avro::NodePtr status_node = root_node->leafAt(0);
    if (status_node->type() != avro::Type::AVRO_INT)
    {
        throw Exception(
            DB::ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `status` field should be Int type, got {}",
            magic_enum::enum_name(status_node->type()));
    }

    avro::NodePtr data_file_node = root_node->leafAt(static_cast<int>(leaves_num) - 1);
    if (data_file_node->type() != avro::Type::AVRO_RECORD)
    {
        throw Exception(
            DB::ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `data_file` field should be Tuple type, got {}",
            magic_enum::enum_name(data_file_node->type()));
    }

    auto status_col_data_type = AvroSchemaReader::avroNodeToDataType(status_node);
    auto data_col_data_type = AvroSchemaReader::avroNodeToDataType(data_file_node);
    Block manifest_file_header
        = {{status_col_data_type->createColumn(), status_col_data_type, "status"},
           {data_col_data_type->createColumn(), data_col_data_type, "data_file"}};

    auto columns = parseAvro(*manifest_file_reader_, manifest_file_header, format_settings);
    if (columns.size() != 2)
        throw Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Unexpected number of columns. Expected 2, got {}", columns.size());

    if (columns.at(0)->getDataType() != TypeIndex::Int32)
    {
        throw Exception(
            DB::ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `status` field should be Int32 type, got {}",
            columns.at(0)->getFamilyName());
    }
    if (columns.at(1)->getDataType() != TypeIndex::Tuple)
    {
        throw Exception(
            DB::ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `file_path` field should be Tuple type, got {}",
            columns.at(1)->getFamilyName());
    }

    const auto * status_int_column = assert_cast<DB::ColumnInt32 *>(columns.at(0).get());
    const auto & data_file_tuple_type = assert_cast<const DataTypeTuple &>(*data_col_data_type.get());
    const auto * data_file_tuple_column = assert_cast<DB::ColumnTuple *>(columns.at(1).get());

    if (status_int_column->size() != data_file_tuple_column->size())
    {
        throw Exception(
            DB::ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `file_path` and `status` have different rows number: {} and {}",
            status_int_column->size(),
            data_file_tuple_column->size());
    }

    ColumnPtr file_path_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName("file_path"));

    if (file_path_column->getDataType() != TypeIndex::String)
    {
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `file_path` field should be String type, got {}",
            file_path_column->getFamilyName());
    }

    const auto * file_path_string_column = assert_cast<const ColumnString *>(file_path_column.get());

    ColumnPtr content_column;
    const ColumnInt32 * content_int_column = nullptr;
    if (format_version_ == 2)
    {
        content_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName("content"));
        if (content_column->getDataType() != TypeIndex::Int32)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `content` field should be Int type, got {}",
                content_column->getFamilyName());
        }

        content_int_column = assert_cast<const ColumnInt32 *>(content_column.get());
    }

    for (size_t i = 0; i < data_file_tuple_column->size(); ++i)
    {
        DataFileContent content_type = DataFileContent::DATA;
        if (format_version_ == 2)
        {
            content_type = DataFileContent(content_int_column->getElement(i));
            if (content_type != DataFileContent::DATA)
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD, "Cannot read Iceberg table: positional and equality deletes are not supported");
        }
        const auto status = ManifestEntryStatus(status_int_column->getInt(i));

        const auto data_path = std::string(file_path_string_column->getDataAt(i).toView());
        const auto pos = data_path.find(common_path);
        if (pos == std::string::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected to find {} in data path: {}", common_path, data_path);

        const auto file_path = data_path.substr(pos);
        this->data_files.push_back({file_path, status, content_type});
    }
}

}

#endif
