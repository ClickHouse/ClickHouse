#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageFactory.h>
#include <Storages/System/StorageSystemTableEngines.h>

namespace DB
{

ColumnsDescription StorageSystemTableEngines::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of table engine."},
        {"supports_settings", std::make_shared<DataTypeUInt8>(), "Flag that indicates if table engine supports SETTINGS clause."},
        {"supports_skipping_indices", std::make_shared<DataTypeUInt8>(), "Flag that indicates if table engine supports skipping indices."},
        {"supports_projections", std::make_shared<DataTypeUInt8>(), "Flag that indicated if table engine supports projections."},
        {"supports_sort_order", std::make_shared<DataTypeUInt8>(),
            "Flag that indicates if table engine supports clauses PARTITION_BY, PRIMARY_KEY, ORDER_BY and SAMPLE_BY."
        },
        {"supports_ttl", std::make_shared<DataTypeUInt8>(), "Flag that indicates if table engine supports TTL."},
        {"supports_replication", std::make_shared<DataTypeUInt8>(), "Flag that indicates if table engine supports data replication."},
        {"supports_deduplication", std::make_shared<DataTypeUInt8>(), "Flag that indicates if table engine supports data deduplication."},
        {"supports_parallel_insert", std::make_shared<DataTypeUInt8>(),
            "Flag that indicates if table engine supports parallel insert (see max_insert_threads setting)."
        },
    };
}

void StorageSystemTableEngines::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
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
