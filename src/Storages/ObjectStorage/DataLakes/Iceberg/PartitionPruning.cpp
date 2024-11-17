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

// #    include <filesystem>

#    include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
namespace DB
{

CommonPartitionInfo
PartitionPruningProcessor::getCommonPartitionInfo(Poco::JSON::Array::Ptr partition_specification, const ColumnTuple * big_partition_tuple)
{
    CommonPartitionInfo common_info;
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

        common_info.partition_columns.push_back(big_partition_tuple->getColumnPtr(i));
        common_info.partition_transforms.push_back(transform);
        common_info.partition_source_ids.push_back(source_id);
    }
    return common_info;
}

SpecificSchemaPartitionInfo PartitionPruningProcessor::getSpecificPartitionPruning(
    const CommonPartitionInfo & common_info,
    [[maybe_unused]] Int32 schema_version,
    const std::unordered_map<Int32, NameAndTypePair> & name_and_type_by_source_id)
{
    SpecificSchemaPartitionInfo specific_info;

    for (size_t i = 0; i < common_info.partition_columns.size(); ++i)
    {
        Int32 source_id = common_info.partition_source_ids[i];
        auto it = name_and_type_by_source_id.find(source_id);
        if (it == name_and_type_by_source_id.end())
        {
            continue;
        }
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
    const SpecificSchemaPartitionInfo & specific_info, const ActionsDAG * filter_dag, ContextPtr context)
{
    std::vector<bool> pruning_mask;
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
                LOG_DEBUG(&Poco::Logger::get("Partition pruning"), "Partition pruning was successful for file: {}", j);
                pruning_mask.push_back(false);
            }
            else
            {
                LOG_DEBUG(&Poco::Logger::get("Partition pruning"), "Partition pruning failed for file: {}", j);
                pruning_mask.push_back(true);
            }
        }
    }
    return pruning_mask;
}

}

#endif
