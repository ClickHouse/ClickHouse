
#include <Storages/ColumnsDescription.h>
#include <Common/CurrentMetrics.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemMetrics.h>


namespace DB
{

ColumnsDescription StorageSystemMetrics::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"metric", std::make_shared<DataTypeString>(), "Metric name."},
        {"value", std::make_shared<DataTypeInt64>(), "Metric value."},
        {"description", std::make_shared<DataTypeString>(), "Metric description."},
    };

    description.setAliases({
        {"name", std::make_shared<DataTypeString>(), "metric"}
    });

    return description;
}

void StorageSystemMetrics::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        Int64 value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

        res_columns[0]->insert(CurrentMetrics::getName(CurrentMetrics::Metric(i)));
        res_columns[1]->insert(value);
        res_columns[2]->insert(CurrentMetrics::getDocumentation(CurrentMetrics::Metric(i)));
    }
}

}
