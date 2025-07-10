#include <Interpreters/Cache/QueryResultCacheFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Core/Settings.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_QUERY_RESULT_CACHE;
    extern const int LOGICAL_ERROR;
}

void QueryResultCacheFactory::registerQueryResultCache(const std::string & name, CreatorFn creator_fn)
{
    if (!caches.emplace(name, std::move(creator_fn)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryResultCacheFactory: the query result cache '{}' is not unique", name);
}

QueryResultCachePtr QueryResultCacheFactory::get(const std::string & name, size_t max_cache_size, const Poco::Util::AbstractConfiguration & config) const
{
    auto creator_it = caches.find(name);
    if (creator_it == caches.end())
        throw Exception(ErrorCodes::UNKNOWN_QUERY_RESULT_CACHE, "The query result cache name {} is not registered.", name);
    return creator_it->second(max_cache_size, config);
}

QueryResultCacheFactory & QueryResultCacheFactory::instance()
{
    static QueryResultCacheFactory ret;
    return ret;
}

}
