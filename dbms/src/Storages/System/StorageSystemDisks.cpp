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

    MutableColumnPtr col_name_mut = ColumnString::create();
    MutableColumnPtr col_path_mut = ColumnString::create();
    MutableColumnPtr col_free_mut = ColumnUInt64::create();
    MutableColumnPtr col_total_mut = ColumnUInt64::create();
    MutableColumnPtr col_keep_mut = ColumnUInt64::create();

    const auto & disk_selector = context.getDiskSelector();

    for (const auto & [name, disk_ptr] : disk_selector.getDisksMap())
    {
        col_name_mut->insert(name);
        col_path_mut->insert(disk_ptr->getPath());
        col_free_mut->insert(disk_ptr->getAvailableSpace());
        col_total_mut->insert(disk_ptr->getTotalSpace());
        col_keep_mut->insert(disk_ptr->getKeepingFreeSpace());
    }

    ColumnPtr col_name = std::move(col_name_mut);
    ColumnPtr col_path = std::move(col_path_mut);
    ColumnPtr col_free = std::move(col_free_mut);
    ColumnPtr col_total = std::move(col_total_mut);
    ColumnPtr col_keep = std::move(col_keep_mut);

    Block res = getSampleBlock().cloneEmpty();
    size_t col_num = 0;
    res.getByPosition(col_num++).column = col_name;
    res.getByPosition(col_num++).column = col_path;
    res.getByPosition(col_num++).column = col_free;
    res.getByPosition(col_num++).column = col_total;
    res.getByPosition(col_num++).column = col_keep;

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(res));
}

}
