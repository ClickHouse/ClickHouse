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

#    include <Storages/ObjectStorage/DataLakes/IcebergMetadata.h>
#    include <DataFile.hh>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

CommonPartitionInfo PartitionPruningProcessor::getCommonPartitionInfo(
    const Poco::JSON::Array::Ptr & partition_specification,
    const ColumnTuple * big_partition_column,
    ColumnPtr file_path_column,
    ColumnPtr status_column,
    const String & manifest_file) const
{
    CommonPartitionInfo common_info;

    // LOG_DEBUG(&Poco::Logger::get("Partition Spec"), "Partition column index: {}", data_file_tuple_type.getPositionByName("partition"));
    // LOG_DEBUG(&Poco::Logger::get("File path Spec"), "File path column index: {}", data_file_tuple_type.getPositionByName("file_path"));

    // const auto * big_partition_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName("partition")).get();

    // common_info.file_path_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName("file_path"));
    common_info.file_path_column = file_path_column;
    common_info.status_column = status_column;
    common_info.manifest_file = manifest_file;
    for (size_t i = 0; i != partition_specification->size(); ++i)
    {
        auto current_field = partition_specification->getObject(static_cast<UInt32>(i));

        auto source_id = current_field->getValue<Int32>("source-id");
        PartitionTransform transform = getTransform(current_field->getValue<String>("transform"));

        if (transform == PartitionTransform::Unsupported)
        {
            continue;
        }
        auto partition_name = current_field->getValue<String>("name");
        LOG_DEBUG(&Poco::Logger::get("Partition Spec"), "Name: {}", partition_name);

        common_info.partition_columns.push_back(big_partition_column->getColumnPtr(i));
        common_info.partition_transforms.push_back(transform);
        common_info.partition_source_ids.push_back(source_id);
    }
    return common_info;
}

SpecificSchemaPartitionInfo PartitionPruningProcessor::getSpecificPartitionInfo(
    const CommonPartitionInfo & common_info,
    [[maybe_unused]] Int32 schema_version,
    const std::unordered_map<Int32, NameAndTypePair> & name_and_type_by_source_id) const
{
    SpecificSchemaPartitionInfo specific_info;

    LOG_DEBUG(&Poco::Logger::get("Form specific info"), "name_and_type_by_source_id size: {}", name_and_type_by_source_id.size());

    for (size_t i = 0; i < common_info.partition_columns.size(); ++i)
    {
        if (common_info.partition_transforms[i] == PartitionTransform::Unsupported
            || common_info.partition_transforms[i] == PartitionTransform::Void)
        {
            continue;
        }
        Int32 source_id = common_info.partition_source_ids[i];
        auto it = name_and_type_by_source_id.find(source_id);
        if (it == name_and_type_by_source_id.end())
        {
            continue;
        }
        LOG_DEBUG(&Poco::Logger::get("Form specific info"), "Added source id: {}", source_id);
        size_t column_size = common_info.partition_columns[i]->size();
        if (specific_info.ranges.empty())
        {
            specific_info.ranges.resize(column_size);
        }
        else
        {
            assert(specific_info.ranges.size() == column_size);
        }
        NameAndTypePair name_and_type = it->second;
        specific_info.partition_names_and_types.push_back(name_and_type);
        for (size_t j = 0; j < column_size; ++j)
        {
            specific_info.ranges[j].push_back(getPartitionRange(
                common_info.partition_transforms[i], static_cast<UInt32>(j), common_info.partition_columns[i], name_and_type.type));
        }
    }
    return specific_info;
}

std::vector<bool> PartitionPruningProcessor::getPruningMask(
    const String & manifest_file,
    const SpecificSchemaPartitionInfo & specific_info,
    const ActionsDAG * filter_dag,
    ContextPtr context) const
{
    std::vector<bool> pruning_mask(specific_info.ranges.size(), true);
    LOG_DEBUG(&Poco::Logger::get("In pruning mask"), "Name and types size: {}", specific_info.partition_names_and_types.size());

    if (!specific_info.partition_names_and_types.empty())
    {
        ExpressionActionsPtr partition_minmax_idx_expr = std::make_shared<ExpressionActions>(
            ActionsDAG(specific_info.partition_names_and_types), ExpressionActionsSettings::fromContext(context));
        const KeyCondition partition_key_condition(
            filter_dag, context, specific_info.partition_names_and_types.getNames(), partition_minmax_idx_expr);
        for (size_t j = 0; j < specific_info.ranges.size(); ++j)
        {
            if (!partition_key_condition.checkInHyperrectangle(specific_info.ranges[j], specific_info.partition_names_and_types.getTypes())
                     .can_be_true)
            {
                LOG_DEBUG(
                    &Poco::Logger::get("Partition pruning"),
                    "Partition pruning in manifest {} was successful for file number {}",
                    manifest_file,
                    j);
                pruning_mask[j] = false;
            }
            else
            {
                LOG_DEBUG(
                    &Poco::Logger::get("Partition pruning"),
                    "Partition pruning in manifest {} failed for file number {}",
                    manifest_file,
                    j);
                pruning_mask[j] = true;
            }
        }
    }
    return pruning_mask;
}

Strings PartitionPruningProcessor::getDataFiles(
    const std::vector<CommonPartitionInfo> & manifest_partitions_infos,
    const std::vector<SpecificSchemaPartitionInfo> & specific_infos,
    const ActionsDAG * filter_dag,
    ContextPtr context,
    const std::string & common_path) const
{
    Strings data_files;
    for (size_t index = 0; index < manifest_partitions_infos.size(); ++index)
    {
        const auto & manifest_partition_info = manifest_partitions_infos[index];
        const auto & specific_partition_info = specific_infos[index];
        size_t number_of_files_in_manifest = manifest_partition_info.file_path_column->size();
        LOG_DEBUG(&Poco::Logger::get("Getting prunned files"), "Filter dag is null: {}", filter_dag == nullptr);
        auto pruning_mask = filter_dag ? getPruningMask(manifest_partition_info.manifest_file, specific_partition_info, filter_dag, context)
                                       : std::vector<bool>{};
        LOG_DEBUG(&Poco::Logger::get("Getting prunned files"), "Number of files in manifest: {}", number_of_files_in_manifest);
        LOG_DEBUG(&Poco::Logger::get("Getting prunned files"), "Number of files in pruning mask: {}", pruning_mask.size());
        chassert(pruning_mask.empty() || number_of_files_in_manifest == pruning_mask.size());
        for (size_t i = 0; i < number_of_files_in_manifest; ++i)
        {
            if (!filter_dag || pruning_mask.empty() || pruning_mask[i])
            {
                const auto status = manifest_partition_info.status_column->getInt(i);
                const auto data_path = std::string(manifest_partition_info.file_path_column->getDataAt(i).toView());
                const auto pos = data_path.find(common_path);
                if (pos == std::string::npos)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected to find {} in data path: {}", common_path, data_path);

                const auto file_path = data_path.substr(pos);

                if (ManifestEntryStatus(status) == ManifestEntryStatus::DELETED)
                {
                    throw Exception(
                        ErrorCodes::UNSUPPORTED_METHOD, "Cannot read Iceberg table: positional and equality deletes are not supported");
                }
                else
                {
                    data_files.push_back(file_path);
                }
            }
        }
    }
    return data_files;
}
}

#endif
