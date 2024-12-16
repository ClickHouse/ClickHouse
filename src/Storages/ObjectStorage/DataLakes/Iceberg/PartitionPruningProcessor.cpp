#include <exception>
#include "Common/DateLUT.h"
#include "Core/NamesAndTypes.h"
#include "config.h"

#if USE_AVRO

#    include <Columns/ColumnString.h>
#    include <Columns/ColumnTuple.h>
#    include <Columns/IColumn.h>
#    include <Core/Settings.h>
#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeDate.h>
#    include <DataTypes/DataTypeDateTime64.h>
#    include <DataTypes/DataTypeFactory.h>
#    include <DataTypes/DataTypeFixedString.h>
#    include <DataTypes/DataTypeMap.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypeTuple.h>
#    include <DataTypes/DataTypeUUID.h>
#    include <DataTypes/DataTypesDecimal.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <Formats/FormatFactory.h>
#    include <IO/ReadBufferFromFileBase.h>
#    include <IO/ReadBufferFromString.h>
#    include <IO/ReadHelpers.h>
#    include <Processors/Formats/Impl/AvroRowInputFormat.h>
#    include <Storages/ObjectStorage/DataLakes/Common.h>
#    include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#    include <Common/logger_useful.h>


#    include <Poco/JSON/Array.h>
#    include <Poco/JSON/Object.h>
#    include <Poco/JSON/Parser.h>

#    include <DataFile.hh>

#    include <Common/ProfileEvents.h>

#    include "Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruning.h"
#    include "Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruningProcessor.h"

namespace ProfileEvents
{
extern const Event IcebergPartitionPrunnedFiles;
}

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

using namespace Iceberg;

// CommonPartitionInfo PartitionPruningProcessor::getCommonPartitionInfo(
//     const Poco::JSON::Array::Ptr & partition_specification,
//     const ColumnTuple * big_partition_column,
//     ColumnPtr file_path_column,
//     ColumnPtr status_column,
//     const String & manifest_file) const
// {
//     CommonPartitionInfo common_info;

//     // LOG_DEBUG(&Poco::Logger::get("Partition Spec"), "Partition column index: {}", data_file_tuple_type.getPositionByName("partition"));
//     // LOG_DEBUG(&Poco::Logger::get("File path Spec"), "File path column index: {}", data_file_tuple_type.getPositionByName("file_path"));

//     // const auto * big_partition_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName("partition")).get();

//     // common_info.file_path_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName("file_path"));
//     common_info.file_path_column = file_path_column;
//     common_info.status_column = status_column;
//     common_info.manifest_file = manifest_file;
//     for (size_t i = 0; i != partition_specification->size(); ++i)
//     {
//         auto current_field = partition_specification->getObject(static_cast<UInt32>(i));

//         auto source_id = current_field->getValue<Int32>("source-id");
//         PartitionTransform transform = getTransform(current_field->getValue<String>("transform"));

//         if (transform == PartitionTransform::Unsupported)
//         {
//             continue;
//         }
//         auto partition_name = current_field->getValue<String>("name");
//         LOG_DEBUG(&Poco::Logger::get("Partition Spec"), "Name: {}", partition_name);

//         common_info.partition_columns.push_back(big_partition_column->getColumnPtr(i));
//         common_info.partition_transforms.push_back(transform);
//         common_info.partition_source_ids.push_back(source_id);
//     }
//     return common_info;
// }

} // namespace DB

#endif
