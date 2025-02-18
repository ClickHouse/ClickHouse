#include <Storages/System/StorageSystemStoragePolicies.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <Disks/IVolume.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Interpreters/Context.h>
#include <base/EnumReflection.h>
#include <QueryPipeline/Pipe.h>


namespace DB
{

namespace ErrorCodes
{
}

namespace
{
    template <typename Type>
    DataTypeEnum8::Values getTypeEnumValues()
    {
        DataTypeEnum8::Values enum_values;
        for (auto value : magic_enum::enum_values<Type>())
            enum_values.emplace_back(magic_enum::enum_name(value), magic_enum::enum_integer(value));
        return enum_values;
    }
}


StorageSystemStoragePolicies::StorageSystemStoragePolicies(const StorageID & table_id_)
        : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(
        ColumnsDescription({
            {"policy_name", std::make_shared<DataTypeString>(), "The name of the storage policy."},
            {"volume_name", std::make_shared<DataTypeString>(), "The name of the volume."},
            {"volume_priority", std::make_shared<DataTypeUInt64>(), "The priority of the volume."},
            {"disks", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The list of all disks names which are a part of this storage policy."},
            {"volume_type", std::make_shared<DataTypeEnum8>(getTypeEnumValues<VolumeType>()), "The type of the volume - JBOD or a single disk."},
            {"max_data_part_size", std::make_shared<DataTypeUInt64>(), "the maximum size of a part that can be stored on any of the volumes disks."},
            {"move_factor", std::make_shared<DataTypeFloat32>(), "When the amount of available space gets lower than this factor, data automatically starts to move on the next volume if any (by default, 0.1)."},
            {"prefer_not_to_merge", std::make_shared<DataTypeUInt8>(), "You should not use this setting. Disables merging of data parts on this volume (this is harmful and leads to performance degradation)."},
            {"perform_ttl_move_on_insert", std::make_shared<DataTypeUInt8>(), "Disables TTL move on data part INSERT. By default (if enabled) if we insert a data part that already expired by the TTL move rule it immediately goes to a volume/disk declared in move rule."},
            {"load_balancing", std::make_shared<DataTypeEnum8>(getTypeEnumValues<VolumeLoadBalancing>()), "Policy for disk balancing, `round_robin` or `least_used`."}
    }));
    // TODO: Add string column with custom volume-type-specific options
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemStoragePolicies::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    MutableColumnPtr col_policy_name = ColumnString::create();
    MutableColumnPtr col_volume_name = ColumnString::create();
    MutableColumnPtr col_priority = ColumnUInt64::create();
    MutableColumnPtr col_disks = ColumnArray::create(ColumnString::create());
    MutableColumnPtr col_volume_type = ColumnInt8::create();
    MutableColumnPtr col_max_part_size = ColumnUInt64::create();
    MutableColumnPtr col_move_factor = ColumnFloat32::create();
    MutableColumnPtr col_prefer_not_to_merge = ColumnUInt8::create();
    MutableColumnPtr col_perform_ttl_move_on_insert = ColumnUInt8::create();
    MutableColumnPtr col_load_balancing = ColumnInt8::create();

    for (const auto & [policy_name, policy_ptr] : context->getPoliciesMap())
    {
        const auto & volumes = policy_ptr->getVolumes();
        for (size_t i = 0; i != volumes.size(); ++i)
        {
            col_policy_name->insert(policy_name);
            col_volume_name->insert(volumes[i]->getName());
            col_priority->insert(i + 1);
            Array disks;
            disks.reserve(volumes[i]->getDisks().size());
            for (const auto & disk_ptr : volumes[i]->getDisks())
                disks.push_back(disk_ptr->getName());
            col_disks->insert(disks);
            col_volume_type->insert(static_cast<Int8>(volumes[i]->getType()));
            col_max_part_size->insert(volumes[i]->max_data_part_size);
            col_move_factor->insert(policy_ptr->getMoveFactor());
            col_prefer_not_to_merge->insert(volumes[i]->areMergesAvoided() ? 1 : 0);
            col_perform_ttl_move_on_insert->insert(volumes[i]->perform_ttl_move_on_insert);
            col_load_balancing->insert(static_cast<Int8>(volumes[i]->load_balancing));
        }
    }

    Columns res_columns;
    res_columns.emplace_back(std::move(col_policy_name));
    res_columns.emplace_back(std::move(col_volume_name));
    res_columns.emplace_back(std::move(col_priority));
    res_columns.emplace_back(std::move(col_disks));
    res_columns.emplace_back(std::move(col_volume_type));
    res_columns.emplace_back(std::move(col_max_part_size));
    res_columns.emplace_back(std::move(col_move_factor));
    res_columns.emplace_back(std::move(col_prefer_not_to_merge));
    res_columns.emplace_back(std::move(col_perform_ttl_move_on_insert));
    res_columns.emplace_back(std::move(col_load_balancing));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(storage_snapshot->metadata->getSampleBlock(), std::move(chunk)));
}

}
