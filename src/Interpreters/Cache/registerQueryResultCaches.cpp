#include <Interpreters/Cache/registerQueryResultCaches.h>
#include <Interpreters/Cache/QueryResultCacheFactory.h>

namespace DB
{

void registerLocalQueryResultCache(QueryResultCacheFactory & factory);
void registerRemoteQueryResultCache(QueryResultCacheFactory & factory);

void registerQueryResultCaches()
{
    auto & factory = QueryResultCacheFactory::instance();

    registerLocalQueryResultCache(factory);
    registerRemoteQueryResultCache(factory);
}

}
