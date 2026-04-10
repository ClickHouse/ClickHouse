#include <DataTypes/DataTypeUUID.h>
#include <Storages/System/StorageSystemQueryConditionCache.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>

#include <shared_mutex>


namespace DB
{

ColumnsDescription StorageSystemQueryConditionCache::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"key_hash", std::make_shared<DataTypeUInt128>(), "Hash of (table_uuid, part_name, condition_hash)."},
#if defined(DEBUG_OR_SANITIZER_BUILD)
        {"table_uuid", std::make_shared<DataTypeUUID>(), "The table UUID. (debug and sanitizer builds only)"},
        {"part_name", std::make_shared<DataTypeString>(), "The part name. (debug and sanitizer builds only)"},
        {"condition", std::make_shared<DataTypeString>(), "The hashed filter condition. (debug and sanitizer builds only)"},
        {"condition_hash", std::make_shared<DataTypeUInt64>(), "The hash of the filter condition. (debug and sanitizer builds only)"},
#endif
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
        ssize_t i = -1;
        res_columns[++i]->insert(key);
#if defined(DEBUG_OR_SANITIZER_BUILD)
        res_columns[++i]->insert(entry->table_id);
        res_columns[++i]->insert(entry->part_name);
        res_columns[++i]->insert(entry->condition);
        res_columns[++i]->insert(entry->condition_hash);
#endif
        res_columns[++i]->insert(QueryConditionCache::EntryWeight()(*entry));

        std::shared_lock lock(entry->mutex);
        res_columns[++i]->insert(to_string(entry->matching_marks));
    }
}

}
