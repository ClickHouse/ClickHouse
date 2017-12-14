#include <Storages/System/StorageSystemAsynchronousMetrics.h>

#include <Interpreters/AsynchronousMetrics.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>


namespace DB
{


StorageSystemAsynchronousMetrics::StorageSystemAsynchronousMetrics(const std::string & name_, const AsynchronousMetrics & async_metrics_)
    : name(name_),
    columns
    {
        {"metric", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeFloat64>()},
    },
    async_metrics(async_metrics_)
{
}


BlockInputStreams StorageSystemAsynchronousMetrics::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context &,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    Block block;

    ColumnWithTypeAndName col_metric;
    col_metric.name = "metric";
    col_metric.type = std::make_shared<DataTypeString>();
    col_metric.column = ColumnString::create();
    block.insert(col_metric);

    ColumnWithTypeAndName col_value;
    col_value.name = "value";
    col_value.type = std::make_shared<DataTypeFloat64>();
    col_value.column = ColumnFloat64::create();
    block.insert(col_value);

    auto async_metrics_values = async_metrics.getValues();

    for (const auto & name_value : async_metrics_values)
    {
        col_metric.column->insert(name_value.first);
        col_value.column->insert(name_value.second);
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
