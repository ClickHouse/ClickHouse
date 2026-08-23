#pragma once

#include <Interpreters/Cache/QueryResultCache.h>

#include <base/types.h>

#include <boost/noncopyable.hpp>
#include <functional>
#include <unordered_map>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

/// Factory for creating IQueryResultCache implementations.
///
/// Register implementations with registerCache() at startup, then call create()
/// to instantiate the type named by the "query_cache.type" config key.
///
/// Built-in registrations (see registerQueryResultCaches.cpp):
///   "local"  → LocalQueryResultCache  (in-process, default)
///   "redis"  → RemoteQueryResultCache (Redis-backed, shared across nodes)
class QueryResultCacheFactory : private boost::noncopyable
{
public:
    using CreatorFn = std::function<
        std::shared_ptr<IQueryResultCache>(const Poco::Util::AbstractConfiguration &)>;

    static QueryResultCacheFactory & instance();

    void registerCache(const String & type_name, CreatorFn fn);

    std::shared_ptr<IQueryResultCache> create(
        const String & type_name,
        const Poco::Util::AbstractConfiguration & config) const;

private:
    std::unordered_map<String, CreatorFn> creators;
};

/// Register all built-in cache types. Call once at server startup.
void registerQueryResultCaches(QueryResultCacheFactory & factory);

}
