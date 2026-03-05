#include <Interpreters/Cache/QueryResultCacheFactory.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Meyers singleton — the factory is created on first access and
/// lives until program termination.
QueryResultCacheFactory & QueryResultCacheFactory::instance()
{
    static QueryResultCacheFactory factory;
    return factory;
}

/// Register a named cache type. Throws if the name is already taken.
void QueryResultCacheFactory::registerCache(const String & type_name, CreatorFn fn)
{
    auto [_, inserted] = creators.emplace(type_name, std::move(fn));
    if (!inserted)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "QueryResultCacheFactory: cache type '{}' is already registered", type_name);
}

/// Look up a registered cache type by name and invoke its creator
/// with the server configuration.
std::shared_ptr<IQueryResultCache> QueryResultCacheFactory::create(
    const String & type_name,
    const Poco::Util::AbstractConfiguration & config) const
{
    auto it = creators.find(type_name);
    if (it == creators.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "QueryResultCacheFactory: unknown cache type '{}'. "
            "Valid types are: local, redis", type_name);
    return it->second(config);
}

}
