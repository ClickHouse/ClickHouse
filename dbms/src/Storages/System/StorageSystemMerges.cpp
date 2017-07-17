#include <Storages/System/StorageSystemMerges.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeList.h>


namespace DB
{

StorageSystemMerges::StorageSystemMerges(const std::string & name)
    : name{name}
    , columns{
        { "database",                         std::make_shared<DataTypeString>() },
        { "table",                            std::make_shared<DataTypeString>() },
        { "elapsed",                        std::make_shared<DataTypeFloat64>() },
        { "progress",                        std::make_shared<DataTypeFloat64>() },
        { "num_parts",                        std::make_shared<DataTypeUInt64>() },
        { "source_part_names",                std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "result_part_name",                std::make_shared<DataTypeString>() },
        { "total_size_bytes_compressed",    std::make_shared<DataTypeUInt64>() },
        { "total_size_marks",                std::make_shared<DataTypeUInt64>() },
        { "bytes_read_uncompressed",        std::make_shared<DataTypeUInt64>() },
        { "rows_read",                        std::make_shared<DataTypeUInt64>() },
        { "bytes_written_uncompressed",     std::make_shared<DataTypeUInt64>() },
        { "rows_written",                    std::make_shared<DataTypeUInt64>() },
        { "columns_written",                std::make_shared<DataTypeUInt64>() },
        { "memory_usage",                    std::make_shared<DataTypeUInt64>() },
        { "thread_number",                    std::make_shared<DataTypeUInt64>() },
    }
{
}


BlockInputStreams StorageSystemMerges::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    Block block = getSampleBlock();

    for (const auto & merge : context.getMergeList().get())
    {
        size_t i = 0;
        block.getByPosition(i++).column->insert(merge.database);
        block.getByPosition(i++).column->insert(merge.table);
        block.getByPosition(i++).column->insert(merge.elapsed);
        block.getByPosition(i++).column->insert(std::min(1., merge.progress)); /// little cheat
        block.getByPosition(i++).column->insert(merge.num_parts);
        block.getByPosition(i++).column->insert(merge.source_part_names);
        block.getByPosition(i++).column->insert(merge.result_part_name);
        block.getByPosition(i++).column->insert(merge.total_size_bytes_compressed);
        block.getByPosition(i++).column->insert(merge.total_size_marks);
        block.getByPosition(i++).column->insert(merge.bytes_read_uncompressed);
        block.getByPosition(i++).column->insert(merge.rows_read);
        block.getByPosition(i++).column->insert(merge.bytes_written_uncompressed);
        block.getByPosition(i++).column->insert(merge.rows_written);
        block.getByPosition(i++).column->insert(merge.columns_written);
        block.getByPosition(i++).column->insert(merge.memory_usage);
        block.getByPosition(i++).column->insert(merge.thread_number);
    }

    return BlockInputStreams{1, std::make_shared<OneBlockInputStream>(block)};
}

}
