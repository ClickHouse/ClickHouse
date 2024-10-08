#include "StorageSystemQueryCache.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cache/QueryCache.h>
#include <Interpreters/Context.h>


namespace DB
{

ColumnsDescription StorageSystemQueryCache::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"query", std::make_shared<DataTypeString>(), "Query string."},
        {"result_size", std::make_shared<DataTypeUInt64>(), "Size of the query cache entry."},
        {"tag", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Tag of the query cache entry."},
        {"stale", std::make_shared<DataTypeUInt8>(), "If the query cache entry is stale."},
        {"shared", std::make_shared<DataTypeUInt8>(), "If the query cache entry is shared between multiple users."},
        {"compressed", std::make_shared<DataTypeUInt8>(), "If the query cache entry is compressed."},
        {"expires_at", std::make_shared<DataTypeDateTime>(), "When the query cache entry becomes stale."},
        {"key_hash", std::make_shared<DataTypeUInt64>(), "A hash of the query string, used as a key to find query cache entries."}
    };
}

StorageSystemQueryCache::StorageSystemQueryCache(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

void StorageSystemQueryCache::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    QueryCachePtr query_cache = context->getQueryCache();

    if (!query_cache)
        return;

    std::vector<QueryCache::Cache::KeyMapped> content = query_cache->dump();

    const String & user_name = context->getUserName();
    std::optional<UUID> user_id = context->getUserID();
    std::vector<UUID> current_user_roles = context->getCurrentRoles();

    for (const auto & [key, query_result] : content)
    {
        /// Showing other user's queries is considered a security risk
        const bool is_same_user_id = ((!key.user_id.has_value() && !user_id.has_value()) || (key.user_id.has_value() && user_id.has_value() && *key.user_id == *user_id));
        const bool is_same_current_user_roles = (key.current_user_roles == current_user_roles);
        if (!key.is_shared && (!is_same_user_id || !is_same_current_user_roles))
            continue;

        res_columns[0]->insert(key.query_string); /// approximates the original query string
        res_columns[1]->insert(QueryCache::QueryCacheEntryWeight()(*query_result));
        res_columns[2]->insert(key.tag);
        res_columns[3]->insert(key.expires_at < std::chrono::system_clock::now());
        res_columns[4]->insert(key.is_shared);
        res_columns[5]->insert(key.is_compressed);
        res_columns[6]->insert(std::chrono::system_clock::to_time_t(key.expires_at));
        res_columns[7]->insert(key.ast_hash.low64); /// query cache considers aliases (issue #56258)
    }
}

}
