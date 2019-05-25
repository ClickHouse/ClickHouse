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
    setColumns(ColumnsDescription(
    {
        {"name", std::make_shared<DataTypeString>()},
        {"volume_priority", std::make_shared<DataTypeUInt64>()},
        {"disks", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"max_data_part_size", std::make_shared<DataTypeUInt64>()},
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

    MutableColumnPtr col_name_mut = ColumnString::create();
    MutableColumnPtr col_priority_mut = ColumnUInt64::create();
    MutableColumnPtr col_disks_mut = ColumnArray::create(ColumnString::create());
    MutableColumnPtr col_max_part_size_mut = ColumnUInt64::create();

    const auto & policy_selector = context.getStoragePolicySelector();

    for (const auto & [name, policy_ptr] : policy_selector.getPoliciesMap())
    {
        const auto & volumes = policy_ptr->getVolumes();
        for (size_t i = 0; i != volumes.size(); ++i)
        {
            col_name_mut->insert(name);
            col_priority_mut->insert(i);
            Array disks;
            disks.reserve(volumes[i].disks.size());
            for (const auto & disk_ptr : volumes[i].disks)
                disks.push_back(disk_ptr->getName());
            col_disks_mut->insert(disks);
            col_max_part_size_mut->insert(volumes[i].max_data_part_size);
        }
    }

    ColumnPtr col_name = std::move(col_name_mut);
    ColumnPtr col_priority = std::move(col_priority_mut);
    ColumnPtr col_disks = std::move(col_disks_mut);
    ColumnPtr col_max_part_size = std::move(col_max_part_size_mut);

    Block res = getSampleBlock().cloneEmpty();
    size_t col_num = 0;
    res.getByPosition(col_num++).column = col_name;
    res.getByPosition(col_num++).column = col_priority;
    res.getByPosition(col_num++).column = col_disks;
    res.getByPosition(col_num++).column = col_max_part_size;

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(res));
}

}
