
#include <Common/CurrentMetrics.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemMetrics.h>


namespace DB
{

NamesAndTypesList StorageSystemMetrics::getNamesAndTypes()
{
    return {
        {"metric", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeInt64>()},
        {"description", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemMetrics::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        Int64 value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

        res_columns[0]->insert(String(CurrentMetrics::getName(CurrentMetrics::Metric(i))));
        res_columns[1]->insert(value);
        res_columns[2]->insert(String(CurrentMetrics::getDocumentation(CurrentMetrics::Metric(i))));
    }
}

}
