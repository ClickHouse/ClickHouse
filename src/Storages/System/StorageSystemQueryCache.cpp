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
        {"result_size", std::make_shared<DataTypeUInt64>()},
        {"stale", std::make_shared<DataTypeUInt8>()},
        {"shared", std::make_shared<DataTypeUInt8>()},
        {"compressed", std::make_shared<DataTypeUInt8>()},
        {"expires_at", std::make_shared<DataTypeDateTime>()},
        {"key_hash", std::make_shared<DataTypeUInt64>()}
    };
}

StorageSystemQueryCache::StorageSystemQueryCache(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_)
{
}

void StorageSystemQueryCache::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    QueryCachePtr query_cache = context->getQueryCache();
    if (!query_cache)
        return;
    std::vector<QueryCache::Cache::KeyMapped> content = query_cache->dump();

    const String & user_name = context->getUserName();
    std::optional<UUID> user_id = context->getUserID();
    std::vector<UUID> current_user_roles = context->getCurrentRolesAsStdVector();

    for (const auto & [key, query_result] : content)
    {
        /// Showing other user's queries is considered a security risk
        const bool is_same_user_id = ((!key.user_id.has_value() && !user_id.has_value()) || (key.user_id.has_value() && user_id.has_value() && *key.user_id == *user_id));
        const bool is_same_current_user_roles = (key.current_user_roles == current_user_roles);
        if (!key.is_shared && (!is_same_user_id || !is_same_current_user_roles))
            continue;

        res_columns[0]->insert(key.query_string); /// approximates the original query string
        res_columns[1]->insert(QueryCache::QueryCacheEntryWeight()(*query_result));
        res_columns[2]->insert(key.expires_at < std::chrono::system_clock::now());
        res_columns[3]->insert(key.is_shared);
        res_columns[4]->insert(key.is_compressed);
        res_columns[5]->insert(std::chrono::system_clock::to_time_t(key.expires_at));
        res_columns[6]->insert(key.ast->getTreeHash().low64);
    }
}

}
