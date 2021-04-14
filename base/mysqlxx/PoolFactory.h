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

    static std::shared_ptr<IPool> getPoolWithFailover(const Poco::Util::AbstractConfiguration & config, const std::string & config_name);

    using HostAndPort = std::pair<std::string, uint16_t>;
    using RemoteDescriptions = std::vector<HostAndPort>;

    static std::shared_ptr<IPool> getPoolWithFailover(
        const std::string & database,
        const RemoteDescriptions & addresses,
        const std::string & user,
        const std::string & password);

    static std::shared_ptr<IPool> getPoolWithFailover(const ReplicasConfigurations & configurations);

    static std::shared_ptr<IPool> getPool(const Poco::Util::AbstractConfiguration & config, const std::string & config_name);

    static std::shared_ptr<IPool> getPool(const ConnectionConfiguration & connection_configuration, const PoolConfiguration & pool_configuration = {});

    static void reset();

    ~PoolFactory() = default;

private:
    PoolFactory();

    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
