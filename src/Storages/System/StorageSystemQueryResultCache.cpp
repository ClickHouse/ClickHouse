#include "StorageSystemQueryResultCache.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/Context.h>


namespace DB
{

ColumnsDescription StorageSystemQueryResultCache::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"query", std::make_shared<DataTypeString>(), "Query string."},
        {"query_id", std::make_shared<DataTypeString>(), "ID of the query."},
        {"result_size", std::make_shared<DataTypeUInt64>(), "Size of the query cache entry."},
        {"tag", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Tag of the query cache entry."},
        {"stale", std::make_shared<DataTypeUInt8>(), "If the query cache entry is stale."},
        {"shared", std::make_shared<DataTypeUInt8>(), "If the query cache entry is shared between multiple users."},
        {"compressed", std::make_shared<DataTypeUInt8>(), "If the query cache entry is compressed."},
        {"expires_at", std::make_shared<DataTypeDateTime>(), "When the query cache entry becomes stale."},
        {"key_hash", std::make_shared<DataTypeUInt64>(), "A hash of the query string, used as a key to find query cache entries."}
    };
}

StorageSystemQueryResultCache::StorageSystemQueryResultCache(const StorageID & table_id)
    : IStorageSystemOneBlock(table_id, getColumnsDescription())
{
}

void StorageSystemQueryResultCache::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    QueryResultCachePtr query_result_cache = context->getQueryResultCache();

    if (!query_result_cache)
        return;

    std::vector<QueryResultCache::Cache::KeyMapped> content = query_result_cache->dump();

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
        res_columns[1]->insert(key.query_id);
        res_columns[2]->insert(QueryResultCache::QueryResultCacheEntryWeight()(*query_result));
        res_columns[3]->insert(key.tag);
        res_columns[4]->insert(key.expires_at < std::chrono::system_clock::now());
        res_columns[5]->insert(key.is_shared);
        res_columns[6]->insert(key.is_compressed);
        res_columns[7]->insert(std::chrono::system_clock::to_time_t(key.expires_at));
        res_columns[8]->insert(key.ast_hash.low64); /// query cache considers aliases (issue #56258)
    }
}

}
