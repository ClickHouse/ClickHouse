#include <Storages/System/StorageSystemQueryResultCache.h>
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

    for (const auto & [key, query_result] : content)
    {
        res_columns[0]->insert(key.query_string); /// approximates the original query string
        res_columns[1]->insert(key.query_id);
        res_columns[2]->insert(QueryResultCache::EntryWeight()(*query_result));
        res_columns[3]->insert(key.tag);
        res_columns[4]->insert(key.expires_at < std::chrono::system_clock::now());
        res_columns[5]->insert(key.is_shared); /// (*)
        res_columns[6]->insert(key.is_compressed);
        res_columns[7]->insert(std::chrono::system_clock::to_time_t(key.expires_at));
        res_columns[8]->insert(key.ast_hash.low64); /// query cache considers aliases (issue #56258)

        /// (*) The query result cache has a setting 'query_cache_share_between_users'. The purpose of the setting is to prevent that query
        ///     results aka. table contents leak to unprivileged users. We intentionally ignore the setting in 'system.query_cache', i.e.
        ///     _all_ cache entries are returned. This is okay because the system table only shows query strings with sensitive data
        ///     removed (the query cache takes care of the latter when an entry is inserted). No query results are shown. Note that this
        ///     behavior is similar to system.query_log.
    }
}

}
