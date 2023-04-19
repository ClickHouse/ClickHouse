#include "StorageSystemQueryResultCache.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/Context.h>


namespace DB
{

NamesAndTypesList StorageSystemQueryResultCache::getNamesAndTypes()
{
    return {
        {"query", std::make_shared<DataTypeString>()},
        {"key_hash", std::make_shared<DataTypeUInt64>()},
        {"expires_at", std::make_shared<DataTypeDateTime>()},
        {"stale", std::make_shared<DataTypeUInt8>()},
        {"shared", std::make_shared<DataTypeUInt8>()},
        {"result_size", std::make_shared<DataTypeUInt64>()}
    };
}

StorageSystemQueryResultCache::StorageSystemQueryResultCache(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_)
{
}

void StorageSystemQueryResultCache::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    auto query_result_cache = context->getQueryResultCache();

    if (!query_result_cache)
        return;

    const String & username = context->getUserName();

    std::lock_guard lock(query_result_cache->mutex);

    for (const auto & [key, result] : query_result_cache->cache)
    {
        /// Showing other user's queries is considered a security risk
        if (key.username.has_value() && key.username != username)
            continue;

        res_columns[0]->insert(key.queryStringFromAst()); /// approximates the original query string
        res_columns[1]->insert(key.ast->getTreeHash().first);
        res_columns[2]->insert(std::chrono::system_clock::to_time_t(key.expires_at));
        res_columns[3]->insert(key.expires_at < std::chrono::system_clock::now());
        res_columns[4]->insert(!key.username.has_value());
        res_columns[5]->insert(result.sizeInBytes());
    }
}

}
