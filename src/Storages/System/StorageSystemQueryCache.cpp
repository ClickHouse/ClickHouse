#include "StorageSystemQueryCache.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cache/QueryCache.h>
#include <Interpreters/Context.h>


namespace DB
{

NamesAndTypesList StorageSystemQueryCache::getNamesAndTypes()
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

StorageSystemQueryCache::StorageSystemQueryCache(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_)
{
}

void StorageSystemQueryCache::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    auto query_cache = context->getQueryCache();

    if (!query_cache)
        return;

    const String & username = context->getUserName();

    std::lock_guard lock(query_cache->mutex);

    for (const auto & [key, result] : query_cache->cache)
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
