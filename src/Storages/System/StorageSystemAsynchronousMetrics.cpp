#include <Columns/IColumn.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/AsynchronousMetrics.h>
#include <Storages/System/StorageSystemAsynchronousMetrics.h>


namespace DB
{

ColumnsDescription StorageSystemAsynchronousMetrics::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"metric", std::make_shared<DataTypeString>(), "Metric name."},
        {"value", std::make_shared<DataTypeFloat64>(), "Metric value. For key-value metrics (broken down per CPU core, block device, disk, ...) it is NaN, and the values are in the `key_values` column."},
        {"key_values", std::make_shared<DataTypeMap>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), std::make_shared<DataTypeFloat64>()), "Values of a key-value metric, e.g. keyed by the CPU core number or the block device name. Empty for scalar metrics."},
        {"description", std::make_shared<DataTypeString>(), "Metric description."},
    };

    description.setAliases({
        {"name", std::make_shared<DataTypeString>(), "metric"}
    });

    return description;
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

        Map key_values;
        key_values.reserve(name_value.second.key_values.size());
        for (const auto & [key, value] : name_value.second.key_values)
            key_values.push_back(Tuple{key, value});
        res_columns[2]->insert(key_values);

        res_columns[3]->insert(name_value.second.documentation);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemAsynchronousMetrics) }
