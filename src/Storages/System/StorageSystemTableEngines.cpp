#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageFactory.h>
#include <Storages/System/StorageSystemTableEngines.h>

namespace DB
{

NamesAndTypesList StorageSystemTableEngines::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"supports_settings", std::make_shared<DataTypeUInt8>()},
        {"supports_skipping_indices", std::make_shared<DataTypeUInt8>()},
        {"supports_projections", std::make_shared<DataTypeUInt8>()},
        {"supports_sort_order", std::make_shared<DataTypeUInt8>()},
        {"supports_ttl", std::make_shared<DataTypeUInt8>()},
        {"supports_replication", std::make_shared<DataTypeUInt8>()},
        {"supports_deduplication", std::make_shared<DataTypeUInt8>()},
        {"supports_parallel_insert", std::make_shared<DataTypeUInt8>()},
    };
}

void StorageSystemTableEngines::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    for (const auto & pair : StorageFactory::instance().getAllStorages())
    {
        int i = 0;
        res_columns[i++]->insert(pair.first);
        res_columns[i++]->insert(pair.second.features.supports_settings);
        res_columns[i++]->insert(pair.second.features.supports_skipping_indices);
        res_columns[i++]->insert(pair.second.features.supports_projections);
        res_columns[i++]->insert(pair.second.features.supports_sort_order);
        res_columns[i++]->insert(pair.second.features.supports_ttl);
        res_columns[i++]->insert(pair.second.features.supports_replication);
        res_columns[i++]->insert(pair.second.features.supports_deduplication);
        res_columns[i++]->insert(pair.second.features.supports_parallel_insert);
    }
}

}
