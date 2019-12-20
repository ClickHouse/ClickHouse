#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemDisks.h>

namespace DB
{

namespace ErrorCodes
{
}


StorageSystemDisks::StorageSystemDisks(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription(
    {
        {"name", std::make_shared<DataTypeString>()},
        {"path", std::make_shared<DataTypeString>()},
        {"free_space", std::make_shared<DataTypeUInt64>()},
        {"total_space", std::make_shared<DataTypeUInt64>()},
        {"keep_free_space", std::make_shared<DataTypeUInt64>()},
    }));
}

BlockInputStreams StorageSystemDisks::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);

    MutableColumnPtr col_name = ColumnString::create();
    MutableColumnPtr col_path = ColumnString::create();
    MutableColumnPtr col_free = ColumnUInt64::create();
    MutableColumnPtr col_total = ColumnUInt64::create();
    MutableColumnPtr col_keep = ColumnUInt64::create();

    const auto & disk_selector = context.getDiskSelector();

    for (const auto & [disk_name, disk_ptr] : disk_selector.getDisksMap())
    {
        col_name->insert(disk_name);
        col_path->insert(disk_ptr->getPath());
        col_free->insert(disk_ptr->getAvailableSpace());
        col_total->insert(disk_ptr->getTotalSpace());
        col_keep->insert(disk_ptr->getKeepingFreeSpace());
    }

    Block res = getSampleBlock().cloneEmpty();
    size_t col_num = 0;
    res.getByPosition(col_num++).column = std::move(col_name);
    res.getByPosition(col_num++).column = std::move(col_path);
    res.getByPosition(col_num++).column = std::move(col_free);
    res.getByPosition(col_num++).column = std::move(col_total);
    res.getByPosition(col_num++).column = std::move(col_keep);

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(res));
}

}
