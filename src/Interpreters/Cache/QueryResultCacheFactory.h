#pragma once

#include <Common/NamePrompter.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/registerStorages.h>
#include <Access/Common/AccessType.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <unordered_map>


namespace DB
{

class Context;
/** Allows to create a query result cache by the name and configurations of cache.
  */
class QueryResultCacheFactory : private boost::noncopyable
{
public:

    static QueryResultCacheFactory & instance();

    using CreatorFn = std::function<QueryResultCachePtr(size_t max_cache_size, const Poco::Util::AbstractConfiguration &)>;
    using QueryResultCaches = std::unordered_map<std::string, CreatorFn>;

    QueryResultCachePtr get(const std::string & name, size_t max_cache_size, const Poco::Util::AbstractConfiguration & config) const;

    /// Register a query result cache by its name.
    /// No locking, you must register all query result cache before usage of get.
    void registerQueryResultCache(const std::string & name, CreatorFn creator_fn);
private:
    QueryResultCaches caches;
};

}
