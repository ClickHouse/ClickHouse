#pragma once

#include <mutex>
#include <memory>
#include <boost/noncopyable.hpp>

#include <mysqlxx/PoolWithFailover.h>


/// NOLINTBEGIN(modernize-macro-to-enum)
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS 1
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS 16
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES 3
/// NOLINTEND(modernize-macro-to-enum)


namespace mysqlxx
{

/** The cache key of a shared pool (`share_connection = 1`): it has to encode every parameter that
  * decides which MySQL instance the pool talks to and as whom (the endpoint, the database, the user,
  * the compression and the TLS credentials), plus the settings of the pool itself. Empty for a pool
  * that is not shared, which is never cached. Exposed for testing.
  */
std::string getPoolEntryName(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_name, unsigned default_max_connections);

/*
 * PoolFactory.h
 * This class is a helper singleton to mutualize connections to MySQL.
 */
class PoolFactory final : private boost::noncopyable
{
public:
    static PoolFactory & instance();

    PoolFactory(const PoolFactory &) = delete;

    /** Allocates a PoolWithFailover to connect to MySQL. */
    PoolWithFailover get(const std::string & config_name,
        unsigned default_connections = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS,
        unsigned max_connections = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS,
        size_t max_tries = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES);

    /** Allocates a PoolWithFailover to connect to MySQL. */
    PoolWithFailover get(const Poco::Util::AbstractConfiguration & config,
        const std::string & config_name,
        unsigned default_connections = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS,
        unsigned max_connections = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS,
        size_t max_tries = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES);

    void reset();


    ~PoolFactory() = default;
    PoolFactory& operator=(const PoolFactory &) = delete;

private:
    PoolFactory();

    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
