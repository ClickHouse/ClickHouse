#include <Storages/System/StorageSystemQueryConditionCache.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>


namespace DB
{

ColumnsDescription StorageSystemQueryConditionCache::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"table_uuid", std::make_shared<DataTypeUUID>(), "The table UUID."},
        {"part_name", std::make_shared<DataTypeString>(), "The part name."},
        {"key_hash", std::make_shared<DataTypeUInt64>(), "The hash of the filter condition."},
        {"entry_size", std::make_shared<DataTypeUInt64>(), "The size of the entry in bytes."},
        {"matching_marks", std::make_shared<DataTypeString>(), "Matching marks."}
    };
}

StorageSystemQueryConditionCache::StorageSystemQueryConditionCache(const StorageID & table_id)
    : IStorageSystemOneBlock(table_id, getColumnsDescription())
{
}

void StorageSystemQueryConditionCache::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    QueryConditionCachePtr query_condition_cache = context->getQueryConditionCache();

    if (!query_condition_cache)
        return;

    std::vector<QueryConditionCache::Cache::KeyMapped> content = query_condition_cache->dump();

    auto to_string = [](const auto & values)
    {
        String str;
        for (auto val : values)
            str += std::to_string(val);
        return str;
    };

    for (const auto & [key, entry] : content)
    {
        res_columns[0]->insert(key.table_id);
        res_columns[1]->insert(key.part_name);
        res_columns[2]->insert(key.condition_hash);
        res_columns[3]->insert(QueryConditionCache::QueryConditionCacheEntryWeight()(*entry));

        std::shared_lock lock(entry->mutex);
        res_columns[4]->insert(to_string(entry->matching_marks));
    }
}

}
