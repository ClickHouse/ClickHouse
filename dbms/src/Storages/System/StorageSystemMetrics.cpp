#include <Common/CurrentMetrics.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemMetrics.h>


namespace DB
{


StorageSystemMetrics::StorageSystemMetrics(const std::string & name_)
    : name(name_),
    columns
    {
        {"metric",         std::make_shared<DataTypeString>()},
        {"value",        std::make_shared<DataTypeInt64>()},
    }
{
}


BlockInputStreams StorageSystemMetrics::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    Block block;

    ColumnWithTypeAndName col_metric;
    col_metric.name = "metric";
    col_metric.type = std::make_shared<DataTypeString>();
    col_metric.column = std::make_shared<ColumnString>();
    block.insert(col_metric);

    ColumnWithTypeAndName col_value;
    col_value.name = "value";
    col_value.type = std::make_shared<DataTypeInt64>();
    col_value.column = std::make_shared<ColumnInt64>();
    block.insert(col_value);

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        Int64 value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

        col_metric.column->insert(String(CurrentMetrics::getDescription(CurrentMetrics::Metric(i))));
        col_value.column->insert(value);
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
