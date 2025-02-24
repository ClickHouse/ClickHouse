#include "config.h"

#if USE_AVRO

#include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileImpl.h"
#include "Storages/ObjectStorage/DataLakes/Iceberg/Utils.h"

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Poco/JSON/Parser.h>
#include "DataTypes/DataTypeTuple.h"

#    include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED_METHOD;
extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace Iceberg
{

const std::vector<ManifestFileEntry> & ManifestFileContent::getFiles() const
{
    return impl->files;
}

Int32 ManifestFileContent::getSchemaId() const
{
    return impl->schema_id;
}

std::vector<DB::Range> ManifestFileEntry::getPartitionRanges(const std::vector<Int32> & partition_columns_ids) const
{
    std::vector<DB::Range> filtered_partition_ranges;
    filtered_partition_ranges.reserve(partition_columns_ids.size());
    for (const auto & partition_column_id : partition_columns_ids)
    {
        filtered_partition_ranges.push_back(partition_ranges.at(partition_column_id));
    }
    return filtered_partition_ranges;
}


const std::vector<PartitionColumnInfo> & ManifestFileContent::getPartitionColumnInfos() const
{
    chassert(impl != nullptr);
    return impl->partition_column_infos;
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
    Int64 inherited_sequence_number)
{
    this->schema_id = schema_id_;

    avro::NodePtr root_node = manifest_file_reader_->dataSchema().root();

    auto [name_to_index, name_to_data_type, manifest_file_header] = getColumnsAndTypesFromAvroByNames(
        root_node, {"status", "data_file", "sequence_number"}, {avro::Type::AVRO_INT, avro::Type::AVRO_RECORD, avro::Type::AVRO_UNION});

    if (name_to_index.find("status") == name_to_index.end())
        throw Exception(DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Required columns are not found in manifest file: status");
    if (name_to_index.find("data_file") == name_to_index.end())
        throw Exception(DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Required columns are not found in manifest file: data_file");
    if (format_version_ > 1 && name_to_index.find("sequence_number") == name_to_index.end())
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Required columns are not found in manifest file: sequence_number");

    auto columns = parseAvro(*manifest_file_reader_, manifest_file_header, format_settings);
    if (columns.at(name_to_index.at("status"))->getDataType() != TypeIndex::Int32)
    {
        throw Exception(
            DB::ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `status` field should be Int32 type, got {}",
            columns.at(name_to_index.at("status"))->getFamilyName());
    }
    if (columns.at(name_to_index.at("data_file"))->getDataType() != TypeIndex::Tuple)
    {
        throw Exception(
            DB::ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `file_path` field should be Tuple type, got {}",
            columns.at(name_to_index.at("data_file"))->getFamilyName());
    }

    const auto * status_int_column = assert_cast<DB::ColumnInt32 *>(columns.at(name_to_index.at("status")).get());

    const auto & data_file_tuple_type = assert_cast<const DataTypeTuple &>(*name_to_data_type.at("data_file").get());
    const auto * data_file_tuple_column = assert_cast<DB::ColumnTuple *>(columns.at(name_to_index.at("data_file")).get());

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
    if (format_version_ > 1)
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


    Poco::JSON::Parser parser;

    ColumnPtr big_partition_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName("partition"));
    if (big_partition_column->getDataType() != TypeIndex::Tuple)
    {
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `partition` field should be Tuple type, got {}",
            big_partition_column->getFamilyName());
    }
    const auto * big_partition_tuple = assert_cast<const ColumnTuple *>(big_partition_column.get());

    auto avro_metadata = manifest_file_reader_->metadata();

    std::vector<uint8_t> partition_spec_json_bytes = avro_metadata["partition-spec"];
    String partition_spec_json_string
        = String(reinterpret_cast<char *>(partition_spec_json_bytes.data()), partition_spec_json_bytes.size());
    Poco::Dynamic::Var partition_spec_json = parser.parse(partition_spec_json_string);
    const Poco::JSON::Array::Ptr & partition_specification = partition_spec_json.extract<Poco::JSON::Array::Ptr>();

    std::vector<ColumnPtr> partition_columns;

    for (size_t i = 0; i != partition_specification->size(); ++i)
    {
        auto current_field = partition_specification->getObject(static_cast<UInt32>(i));

        auto source_id = current_field->getValue<Int32>("source-id");
        PartitionTransform transform = getTransform(current_field->getValue<String>("transform"));

        if (transform == PartitionTransform::Unsupported || transform == PartitionTransform::Void)
        {
            continue;
        }

        partition_column_infos.emplace_back(transform, source_id);
        partition_columns.push_back(removeNullable(big_partition_tuple->getColumnPtr(i)));
    }

    std::optional<const ColumnNullable *> sequence_number_column = std::nullopt;
    if (format_version_ > 1)
    {
        if (columns.at(name_to_index.at("sequence_number"))->getDataType() != TypeIndex::Nullable)
        {
            throw Exception(
                DB::ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `sequence_number` field should be Int64 type, got {}",
                columns.at(name_to_index.at("sequence_number"))->getFamilyName());
        }
        sequence_number_column = assert_cast<const ColumnNullable *>(columns.at(name_to_index.at("sequence_number")).get());
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

        const auto data_path = std::string(file_path_string_column->getDataAt(i).toView());
        const auto pos = data_path.find(common_path);
        if (pos == std::string::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected to find {} in data path: {}", common_path, data_path);

        const auto file_path = data_path.substr(pos);
        std::unordered_map<Int32, Range> partition_ranges;
        for (size_t j = 0; j < partition_columns.size(); ++j)
        {
            const Int32 source_id = partition_column_infos[j].source_id;
            partition_ranges.emplace(
                source_id,
                getPartitionRange(
                    partition_column_infos[j].transform,
                    i,
                    partition_columns[j],
                    schema_processor.getFieldCharacteristics(schema_id, source_id).type));
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
                            "Data sequence number is null for the file added in another shapshot");
                    else
                        added_sequence_number = sequence_number_column.value()->getInt(i);
                    break;
                case ManifestEntryStatus::DELETED:
                    added_sequence_number = inherited_sequence_number;
                    break;
            }
        }
        this->files.emplace_back(status, added_sequence_number, partition_ranges, file);
    }
}

}

#endif
