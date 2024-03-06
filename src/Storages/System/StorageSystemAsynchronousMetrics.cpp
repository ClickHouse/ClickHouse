#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/AsynchronousMetrics.h>
#include <Storages/System/StorageSystemAsynchronousMetrics.h>


namespace DB
{

ColumnsDescription StorageSystemAsynchronousMetrics::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"metric", std::make_shared<DataTypeString>(), "Metric name."},
        {"value", std::make_shared<DataTypeFloat64>(), "Metric value."},
        {"description", std::make_shared<DataTypeString>(), "Metric description."},
    };
}


StorageSystemAsynchronousMetrics::StorageSystemAsynchronousMetrics(const StorageID & table_id_, const AsynchronousMetrics & async_metrics_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription()), async_metrics(async_metrics_)
{
}

void StorageSystemAsynchronousMetrics::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto async_metrics_values = async_metrics.getValues();
    for (const auto & name_value : async_metrics_values)
    {
        res_columns[0]->insert(name_value.first);
        res_columns[1]->insert(name_value.second.value);
        res_columns[2]->insert(name_value.second.documentation);
    }
}

}
