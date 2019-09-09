#include <Columns/ColumnArray.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemStoragePolicies.h>
#include <DataTypes/DataTypeArray.h>


namespace DB
{

namespace ErrorCodes
{
}


StorageSystemStoragePolicies::StorageSystemStoragePolicies(const std::string & name_)
        : name(name_)
{
    setColumns(
        ColumnsDescription({
             {"policy_name", std::make_shared<DataTypeString>()},
             {"volume_name", std::make_shared<DataTypeString>()},
             {"volume_priority", std::make_shared<DataTypeUInt64>()},
             {"disks", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
             {"max_data_part_size", std::make_shared<DataTypeUInt64>()},
             {"move_factor", std::make_shared<DataTypeFloat32>()}
    }));
}

BlockInputStreams StorageSystemStoragePolicies::read(
        const Names & column_names,
        const SelectQueryInfo & /*query_info*/,
        const Context & context,
        QueryProcessingStage::Enum /*processed_stage*/,
        const size_t /*max_block_size*/,
        const unsigned /*num_streams*/)
{
    check(column_names);

    MutableColumnPtr col_policy_name = ColumnString::create();
    MutableColumnPtr col_volume_name = ColumnString::create();
    MutableColumnPtr col_priority = ColumnUInt64::create();
    MutableColumnPtr col_disks = ColumnArray::create(ColumnString::create());
    MutableColumnPtr col_max_part_size = ColumnUInt64::create();
    MutableColumnPtr col_move_factor = ColumnFloat32::create();

    const auto & policy_selector = context.getStoragePolicySelector();

    for (const auto & [policy_name, policy_ptr] : policy_selector.getPoliciesMap())
    {
        const auto & volumes = policy_ptr->getVolumes();
        for (size_t i = 0; i != volumes.size(); ++i)
        {
            col_policy_name->insert(policy_name);
            col_volume_name->insert(volumes[i]->getName());
            col_priority->insert(i + 1);
            Array disks;
            disks.reserve(volumes[i]->disks.size());
            for (const auto & disk_ptr : volumes[i]->disks)
                disks.push_back(disk_ptr->getName());
            col_disks->insert(disks);
            col_max_part_size->insert(volumes[i]->max_data_part_size);
            col_move_factor->insert(policy_ptr->getMoveFactor());
        }
    }


    Block res = getSampleBlock().cloneEmpty();

    size_t col_num = 0;
    res.getByPosition(col_num++).column = std::move(col_policy_name);
    res.getByPosition(col_num++).column = std::move(col_volume_name);
    res.getByPosition(col_num++).column = std::move(col_priority);
    res.getByPosition(col_num++).column = std::move(col_disks);
    res.getByPosition(col_num++).column = std::move(col_max_part_size);
    res.getByPosition(col_num++).column = std::move(col_move_factor);

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(res));
}

}
