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
    async_metrics(async_metrics_)
{
    setColumns(ColumnsDescription({
        {"metric", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeFloat64>()},
    }));
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

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

    auto async_metrics_values = async_metrics.getValues();

    for (const auto & name_value : async_metrics_values)
    {
        res_columns[0]->insert(name_value.first);
        res_columns[1]->insert(name_value.second);
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


}
