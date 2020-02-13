#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Storages/System/StorageSystemAsynchronousMetrics.h>


namespace DB
{

NamesAndTypesList StorageSystemAsynchronousMetrics::getNamesAndTypes()
{
    return {
        {"metric", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeFloat64>()},
    };
}


StorageSystemAsynchronousMetrics::StorageSystemAsynchronousMetrics(const std::string & name_, const AsynchronousMetrics & async_metrics_)
    : IStorageSystemOneBlock(name_), async_metrics(async_metrics_)
{
}

void StorageSystemAsynchronousMetrics::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    auto async_metrics_values = async_metrics.getValues();
    for (const auto & name_value : async_metrics_values)
    {
        res_columns[0]->insert(name_value.first);
        res_columns[1]->insert(name_value.second);
    }
}

}
