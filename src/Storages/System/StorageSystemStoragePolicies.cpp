#include <Storages/System/StorageSystemStoragePolicies.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
}


StorageSystemStoragePolicies::StorageSystemStoragePolicies(const std::string & name_)
        : IStorage({"system", name_})
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(
        ColumnsDescription({
             {"policy_name", std::make_shared<DataTypeString>()},
             {"volume_name", std::make_shared<DataTypeString>()},
             {"volume_priority", std::make_shared<DataTypeUInt64>()},
             {"disks", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
             {"volume_type", std::make_shared<DataTypeString>()},
             {"max_data_part_size", std::make_shared<DataTypeUInt64>()},
             {"move_factor", std::make_shared<DataTypeFloat32>()},
    }));
    // TODO: Add string column with custom volume-type-specific options
    setInMemoryMetadata(storage_metadata);
}

Pipes StorageSystemStoragePolicies::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    MutableColumnPtr col_policy_name = ColumnString::create();
    MutableColumnPtr col_volume_name = ColumnString::create();
    MutableColumnPtr col_priority = ColumnUInt64::create();
    MutableColumnPtr col_disks = ColumnArray::create(ColumnString::create());
    MutableColumnPtr col_volume_type = ColumnString::create();
    MutableColumnPtr col_max_part_size = ColumnUInt64::create();
    MutableColumnPtr col_move_factor = ColumnFloat32::create();

    for (const auto & [policy_name, policy_ptr] : context.getPoliciesMap())
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
            col_volume_type->insert(volumeTypeToString(volumes[i]->getType()));
            col_max_part_size->insert(volumes[i]->max_data_part_size);
            col_move_factor->insert(policy_ptr->getMoveFactor());
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

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    Pipes pipes;
    pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(metadata_snapshot->getSampleBlock(), std::move(chunk)));

    return pipes;
}

}
