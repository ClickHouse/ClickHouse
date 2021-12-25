#pragma once

#include <mutex>
#include <memory>
#include <boost/noncopyable.hpp>

#include <mysqlxx/PoolWithFailover.h>


#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS 1
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS 16
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES 3


namespace mysqlxx
{

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
